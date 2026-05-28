package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapBoolean;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.duckdb.*;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * DuckDB SQL 处理器节点
 * 继承 HazelcastProcessorBaseNode，支持流式处理模式
 * 使用 Arrow 零拷贝写入实现高性能数据处理
 */
public class HazelcastDuckDbSqlNode extends HazelcastProcessorBaseNode {

    private static final Logger logger = LogManager.getLogger(HazelcastDuckDbSqlNode.class);
    public static final String TAG = HazelcastDuckDbSqlNode.class.getSimpleName();

    /** DuckDB 操作器 */
    private DuckDbOperator duckDbOperator;
    private static final int DEFAULT_BATCH_SIZE = 1000;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private static final String DEFAULT_SOURCE_ID = "default_source";
    private static final int DEFAULT_MAX_ACTIVE_SOURCES = 50;
    private static final int DEFAULT_COMMIT_INTERVAL_MS = 5000;
    private static final String DLQ_COLLECTION = "duckdb_dlq_records";
    private final Map<String, PerSourceContext> sourceContexts = new ConcurrentHashMap<>();
    private final LinkedHashMap<String, Boolean> sourceAccessOrder = new LinkedHashMap<>(16, 0.75f, true);
    private final Object sourceContextLock = new Object();
    private final int maxActiveSources = DEFAULT_MAX_ACTIVE_SOURCES;
    private final int commitIntervalMs = DEFAULT_COMMIT_INTERVAL_MS;
    private ScheduledExecutorService contextFlusher;
    private DlqWriter dlqWriter;

    // Integration helpers for full/CDC flow
    private MultiTableInputManager multiTableInputManager;
    private SyncStageTracker syncStageTracker;
    private OutputBuffer outputBuffer;
    private DuckDbQueryEngine queryEngine;
    private ErrorHandler errorHandler;
    private SchemaRegistry schemaRegistry;
    // ========== 新增: 实时增量物化视图组件 ==========
    private AffectedKeyCalculator affectedKeyCalculator;
    private WideTableIncrementalUpdater wideTableUpdater;
    // @Deprecated: 使用 wideTableUpdater 替代
    private IncrementalViewUpdater incrementalViewUpdater;

    private static final int ERROR_THRESHOLD_COUNT = 100;
    private static final double ERROR_THRESHOLD_RATE = 0.01; // 1%
    private static final int OUTPUT_BATCH_SIZE = 1000;
    private static final int QUERY_TIMEOUT_MS = 5000;

    // SQL configuration
    private String querySql = "SELECT * FROM %s";
    private String outputTableName = "duckdb_output";
    private boolean executeQueryOnFullSyncComplete = true;
    private volatile boolean queryExecuted = false;

    // ========== 新增: 实时增量物化视图配置 ==========
    private String wideTablePrimaryKey;
    private boolean outputChangelogEnabled = false;
    private String mainTableName;
    private String mainTablePrimaryKey;
    private List<FromTableConfig> fromTables = new ArrayList<>();
    private Map<String, String> customJoinQueries = new HashMap<>();



    // ========== 新增: 获取表主键的辅助方法 ==========
    public String getTablePrimaryKey(String tableName) {
        if (tableName.equals(mainTableName)) {
            return mainTablePrimaryKey;
        }
        if (fromTables != null) {
            for (FromTableConfig fromTable : fromTables) {
                if (fromTable.getTableName().equals(tableName)) {
                    return fromTable.getPrimaryKey();
                }
            }
        }
        return null;
    }

    public String getCustomJoinQuery(String fromTableName, java.util.Set<Object> fromTablePks) {
        String template = customJoinQueries.get(fromTableName);
        if (template != null) {
            return template.replace("${pkValues}", fromTablePks.stream()
                    .map(String::valueOf)
                    .collect(java.util.stream.Collectors.joining(",")));
        }
        throw new IllegalStateException("No custom join query configured for table: " + fromTableName);
    }

    // Pending events to emit
    private final Queue<TapdataEvent> pendingEvents = new LinkedList<>();
    private BiConsumer<TapdataEvent, ProcessResult> currentConsumer;

    public HazelcastDuckDbSqlNode(ProcessorBaseContext processorBaseContext) {
        super(processorBaseContext);
    }

    @Override
    protected void doInit(@NotNull Context context) throws TapCodeException {
        super.doInit(context);
        
        DuckLakeConfig duckLakeConfig = DuckLakeConfig.disabled();

        // 读取节点配置
        try {
            com.tapdata.tm.commons.dag.process.DuckDbSqlNode nodeConfig =
                    (com.tapdata.tm.commons.dag.process.DuckDbSqlNode) getNode();

            if (nodeConfig != null) {
                // 读取 SQL 查询
                if (nodeConfig.getQuerySql() != null) {
                    this.querySql = nodeConfig.getQuerySql();
                }
                
                // 验证 SQL 必须是 SELECT 查询语句
                DuckDbOperator.ensureSelectQuery(this.querySql, "DuckDbSqlNode querySql configuration");

                // 读取输出表名
                if (nodeConfig.getOutputTableName() != null) {
                    this.outputTableName = nodeConfig.getOutputTableName();
                }

                // 读取批大小
                if (nodeConfig.getBatchSize() != null) {
                    this.batchSize = nodeConfig.getBatchSize();
                }

                // 读取是否在全量结束后执行查询
                if (nodeConfig.getExecuteQueryOnFullSyncComplete() != null) {
                    this.executeQueryOnFullSyncComplete = nodeConfig.getExecuteQueryOnFullSyncComplete();
                }
                
                // 读取 DuckLake 配置
                if (Boolean.TRUE.equals(nodeConfig.getDuckLakeEnabled())) {
                    duckLakeConfig = new io.tapdata.flow.engine.V2.node.duckdb.DuckLakeConfig(
                            true,
                            nodeConfig.getDuckLakeStorageType(),
                            nodeConfig.getDuckLakeStoragePath(),
                            nodeConfig.getDuckLakeMetadataDbUrl()
                    );
                }

                // ========== 新增: 读取实时增量物化视图配置 ==========
                if (nodeConfig.getWideTablePrimaryKey() != null) {
                    this.wideTablePrimaryKey = nodeConfig.getWideTablePrimaryKey();
                }
                if (nodeConfig.getOutputChangelogEnabled() != null) {
                    this.outputChangelogEnabled = nodeConfig.getOutputChangelogEnabled();
                }
                if (nodeConfig.getMainTableName() != null) {
                    this.mainTableName = nodeConfig.getMainTableName();
                }
                if (nodeConfig.getMainTablePrimaryKey() != null) {
                    this.mainTablePrimaryKey = nodeConfig.getMainTablePrimaryKey();
                }
                if (nodeConfig.getFromTables() != null) {
                    // 将 Manager 端配置转换为 Engine 端 FromTableConfig
                    this.fromTables = nodeConfig.getFromTables().stream()
                            .map(ft -> {
                                FromTableConfig config = new FromTableConfig();
                                try {
                                    config.setTableName((String) ft.getClass().getMethod("getTableName").invoke(ft));
                                    config.setPrimaryKey((String) ft.getClass().getMethod("getPrimaryKey").invoke(ft));
                                } catch (Exception e) {
                                    logger.warn("Failed to convert FromTableConfig: {}", e.getMessage());
                                }
                                return config;
                            })
                            .collect(java.util.stream.Collectors.toList());
                }
                if (nodeConfig.getCustomJoinQueries() != null) {
                    this.customJoinQueries = new HashMap<>(nodeConfig.getCustomJoinQueries());
                }

                logger.info("DuckDbSqlNode loaded config: querySql={}, outputTableName={}, batchSize={}, executeQueryOnFullSyncComplete={}, duckLake={}, materializedView={}",
                        querySql, outputTableName, batchSize, executeQueryOnFullSyncComplete, duckLakeConfig.isEnabled(),
                        wideTablePrimaryKey != null ? "enabled" : "disabled");
            }
        } catch (Exception e) {
            logger.warn("Failed to load DuckDbSqlNode config, using defaults: {}", e.getMessage());
        }

        // 初始化 DuckDB 操作器（内存数据库模式）
        try {
            duckDbOperator = new io.tapdata.flow.engine.V2.node.duckdb.DuckDbOperatorImpl(true, batchSize, 5000, duckLakeConfig);
            contextFlusher = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread thread = new Thread(r, "duckdb-multi-source-flusher");
                thread.setDaemon(true);
                return thread;
            });
            contextFlusher.scheduleAtFixedRate(this::flushAllContextsSafely, commitIntervalMs, commitIntervalMs, TimeUnit.MILLISECONDS);
            dlqWriter = new DlqWriter(clientMongoOperator, DLQ_COLLECTION);

            // 初始化全量/CDC集成辅助组件
            multiTableInputManager = new MultiTableInputManager();
            syncStageTracker = new SyncStageTracker();
            outputBuffer = new OutputBuffer(OUTPUT_BATCH_SIZE);
            queryEngine = new DuckDbQueryEngine(1024, QUERY_TIMEOUT_MS);
            errorHandler = new ErrorHandler(ERROR_THRESHOLD_COUNT, ERROR_THRESHOLD_RATE);
            schemaRegistry = new SchemaRegistry();

            // ========== 新增: 初始化实时增量物化视图组件 ==========
            if (wideTablePrimaryKey != null && !wideTablePrimaryKey.isEmpty()) {
                // 初始化 AffectedKeyCalculator
                affectedKeyCalculator = new AffectedKeyCalculator(
                        wideTablePrimaryKey,
                        mainTableName,
                        mainTablePrimaryKey,
                        fromTables, // 直接使用我们已经转换好的 fromTables
                        customJoinQueries,
                        duckDbOperator
                );

                // 根据全局配置选择新组件或旧组件
                if (DuckDbSqlConfig.isUseNewWideTableUpdater()) {
                    // 新组件：WideTableIncrementalUpdater（事务模式）
                    wideTableUpdater = new WideTableIncrementalUpdater(
                            "wide_table",
                            wideTablePrimaryKey,
                            querySql,
                            extractFieldsFromQuery(),
                            new WithCteSqlGenerator(),
                            duckDbOperator,
                            true // 启用事务模式
                    );

                    // 注册 Changelog 监听器
                    if (outputChangelogEnabled) {
                        wideTableUpdater.addChangelogListener(event -> {
                            logger.debug("Generated changelog event: {}", event);
                            // TODO: 将 TapdataEvent 转换为 TapRecordEvent 并输出
                        });
                    }

                    logger.info("Using NEW WideTableIncrementalUpdater (batch SQL mode)");
                } else {
                    // 旧组件：IncrementalViewUpdater（fallback）
                    incrementalViewUpdater = new IncrementalViewUpdater(
                            outputTableName,
                            wideTablePrimaryKey,
                            querySql,
                            outputChangelogEnabled,
                            duckDbOperator
                    );

                    if (outputChangelogEnabled) {
                        incrementalViewUpdater.addChangelogListener(changelogEvent -> {
                            logger.debug("Generated changelog event (legacy): {}", changelogEvent);
                        });
                    }

                    logger.warn("Using DEPRECATED IncrementalViewUpdater - consider switching to new component");
                }

                logger.info("Materialized view components initialized: affectedKeyCalculator={}, wideTableUpdater={}, incrementalViewUpdater={}",
                        affectedKeyCalculator != null, wideTableUpdater != null, incrementalViewUpdater != null);
            }

            // Register callback for when all tables enter CDC
            syncStageTracker.setOnAllTablesCdcCallback(v -> {
                try {
                    handleAllTablesCdcTransition();
                } catch (Exception e) {
                    logger.error("Error handling all tables CDC transition: {}", e.getMessage(), e);
                }
            });

            logger.info("DuckDbSqlNode initialized with batchSize={}, output batch size={}, error threshold={}% (count: {})",
                    batchSize, OUTPUT_BATCH_SIZE, ERROR_THRESHOLD_RATE * 100, ERROR_THRESHOLD_COUNT);
        } catch (SQLException e) {
            throw new TapCodeException("Failed to initialize DuckDbOperator", e);
        }
    }

    /**
     * 处理所有表进入CDC阶段的转换
     * 当所有表都从全量同步切换到增量同步时触发
     */
    private void handleAllTablesCdcTransition() {
        logger.info("所有表已切换到CDC阶段，执行查询并发射结果...");

        if (executeQueryOnFullSyncComplete && !queryExecuted) {
            try {
                // 步骤1: 刷写所有剩余数据
                flushAllContexts();

                // 步骤2: 执行查询并发射结果
                executeAndEmitQueryResults();

                queryExecuted = true;
                logger.info("查询执行成功，结果已发射");
            } catch (Exception e) {
                logger.error("全量同步完成后执行查询失败: {}", e.getMessage(), e);
            }
        }
    }

    /**
     * 执行SQL查询并将结果作为TapdataEvent发射
     */
    private void executeAndEmitQueryResults() {
        try {
            // 步骤1: 确定需要查询的表
            List<String> tableNames = new ArrayList<>();
            for (PerSourceContext context : sourceContexts.values()) {
                if (context.getTargetTableName() != null) {
                    tableNames.add(context.getTargetTableName());
                }
            }

            if (tableNames.isEmpty()) {
                logger.warn("没有可查询的表，跳过结果发射");
                return;
            }

            // 步骤2: 对每个表执行查询
            for (String tableName : tableNames) {
                String sql = String.format(querySql, tableName);
                logger.info("执行表 {} 的查询: {}", tableName, sql);

                DuckDbOperator.ExecuteResult executeResult = duckDbOperator.execute(sql);

                if (executeResult.isHasResultSet()) {
                    List<Map<String, Object>> results = executeResult.getResultSet();
                    if (results != null && !results.isEmpty()) {
                        logger.info("表 {} 查询返回 {} 条结果", tableName, results.size());

                        // 将结果作为TapInsertRecordEvent发射
                        for (Map<String, Object> result : results) {
                            emitResultAsTapEvent(result, tableName);
                        }
                    } else {
                        logger.info("表 {} 查询无结果", tableName);
                    }
                } else {
                    logger.info("表 {} SQL执行成功，影响行数: {}", tableName, executeResult.getUpdateCount());
                }
            }

        } catch (Exception e) {
            logger.error("执行查询并发射结果失败: {}", e.getMessage(), e);
        }
    }

    /**
     * 将单条结果作为TapdataEvent发射
     */
    private void emitResultAsTapEvent(Map<String, Object> result, String tableName) {
        try {
            // 创建TapInsertRecordEvent
            TapInsertRecordEvent insertEvent =
                    new TapInsertRecordEvent();
            insertEvent.setTableId(tableName);
            insertEvent.setAfter(result);

            // 创建TapdataEvent并设置同步阶段为CDC
            TapdataEvent tapdataEvent = new TapdataEvent();
            tapdataEvent.setTapEvent(insertEvent);
            tapdataEvent.setSyncStage(com.tapdata.entity.SyncStage.CDC);

            // 添加到待处理事件队列
            pendingEvents.offer(tapdataEvent);

            logger.debug("已将查询结果添加到待处理事件队列, 表: {}", tableName);

        } catch (Exception e) {
            logger.warn("发射查询结果为TapEvent失败: {}", e.getMessage());
        }
    }

    public void setQuerySql(String querySql) {
        DuckDbOperator.ensureSelectQuery(querySql, "DuckDbSqlNode querySql setter");
        this.querySql = querySql;
    }

    public void setOutputTableName(String outputTableName) {
        this.outputTableName = outputTableName;
    }

    public void setExecuteQueryOnFullSyncComplete(boolean executeQueryOnFullSyncComplete) {
        this.executeQueryOnFullSyncComplete = executeQueryOnFullSyncComplete;
    }

    // ========== 新增: 实时增量物化视图的 getter/setter ==========
    public String getWideTablePrimaryKey() {
        return wideTablePrimaryKey;
    }

    public void setWideTablePrimaryKey(String wideTablePrimaryKey) {
        this.wideTablePrimaryKey = wideTablePrimaryKey;
    }

    public boolean isOutputChangelogEnabled() {
        return outputChangelogEnabled;
    }

    public void setOutputChangelogEnabled(boolean outputChangelogEnabled) {
        this.outputChangelogEnabled = outputChangelogEnabled;
    }

    public String getMainTableName() {
        return mainTableName;
    }

    public void setMainTableName(String mainTableName) {
        this.mainTableName = mainTableName;
    }

    public String getMainTablePrimaryKey() {
        return mainTablePrimaryKey;
    }

    public void setMainTablePrimaryKey(String mainTablePrimaryKey) {
        this.mainTablePrimaryKey = mainTablePrimaryKey;
    }

    public List<FromTableConfig> getFromTables() {
        return fromTables;
    }

    public void setFromTables(List<FromTableConfig> fromTables) {
        this.fromTables = fromTables;
    }

    public Map<String, String> getCustomJoinQueries() {
        return customJoinQueries;
    }

    public void setCustomJoinQueries(Map<String, String> customJoinQueries) {
        this.customJoinQueries = customJoinQueries;
    }

    /**
     * 从查询 SQL 中提取字段列表
     * 简单实现：解析 SELECT 后的字段名
     */
    private List<String> extractFieldsFromQuery() {
        // 简单解析：SELECT field1, field2 FROM ...
        String upperSql = querySql.toUpperCase();
        int selectIndex = upperSql.indexOf("SELECT");
        int fromIndex = upperSql.indexOf("FROM");
        
        if (selectIndex >= 0 && fromIndex > selectIndex) {
            String fieldsPart = querySql.substring(selectIndex + 6, fromIndex).trim();
            // 按逗号分割，去除空格
            return Arrays.stream(fieldsPart.split(","))
                    .map(String::trim)
                    .filter(f -> !f.isEmpty())
                    .collect(java.util.stream.Collectors.toList());
        }
        
        // fallback: 返回空列表，由 WithCteSqlGenerator 处理
        return Collections.emptyList();
    }

    /**
     * 处理数据事件的核心方法
     * 负责事件分类、错误监控、同步阶段追踪和事件分发
     * 
     * @param tapdataEvent 数据事件
     * @param consumer 事件消费者
     */
    @Override
    protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
        // 步骤1: 保存当前消费者用于发送待处理事件
        currentConsumer = consumer;

        // 步骤2: 发送待处理事件
        emitPendingEvents();

        // 步骤3: 空事件检查
        if (tapdataEvent == null) {
            return;
        }

        TapEvent tapEvent = tapdataEvent.getTapEvent();

        // 步骤4: 错误率监控
        if (errorHandler != null) {
            errorHandler.recordEvent();

            // 检查是否超过错误阈值需要停止任务
            if (errorHandler.shouldStopTask()) {
                logger.error("超过错误阈值，任务应停止");
                return;
            }
        }

        // 步骤5: 同步阶段追踪
        trackSyncStage(tapdataEvent);

        // 步骤6: 事件分类处理
        if (tapEvent instanceof TapRecordEvent) {
            // DML事件: 统一处理
            processRecordEvent((TapRecordEvent) tapEvent, tapdataEvent, consumer);
        } else {
            // 非DML事件: 直接透传
            consumer.accept(tapdataEvent, null);
        }
    }

    /**
     * 发送所有待处理事件
     * 用于将查询结果等延迟发送的事件传递给下游
     */
    private void emitPendingEvents() {
        if (currentConsumer != null && !pendingEvents.isEmpty()) {
            TapdataEvent event;
            while ((event = pendingEvents.poll()) != null) {
                try {
                    currentConsumer.accept(event, null);
                } catch (Exception e) {
                    logger.warn("发送待处理事件失败: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * 追踪数据事件的同步阶段
     * 记录每个表当前所处的同步阶段(全量/CDC)
     * 
     * @param tapdataEvent 数据事件
     */
    private void trackSyncStage(TapdataEvent tapdataEvent) {
        if (syncStageTracker != null && tapdataEvent != null) {
            SyncStage stage = tapdataEvent.getSyncStage();
            TapEvent tapEvent = tapdataEvent.getTapEvent();

            if (tapEvent instanceof TapRecordEvent) {
                String tableName = TapEventUtil.getTableId((TapRecordEvent) tapEvent);
                if (tableName != null) {
                    syncStageTracker.updateTableStageFromEvent(tableName, stage);
                }
            }
        }
    }

    /**
     * 处理记录事件（统一方法）
     * 根据同步阶段决定是否执行CDC物化视图逻辑
     * 
     * @param recordEvent 记录事件
     * @param tapdataEvent 数据事件
     * @param consumer 事件消费者
     */
    private void processRecordEvent(TapRecordEvent recordEvent, TapdataEvent tapdataEvent,
                                    BiConsumer<TapdataEvent, ProcessResult> consumer) {
        try {
            // ========== 步骤1: 获取上下文 ==========
            String tableId = TapEventUtil.getTableId(recordEvent);
            String sourceId = resolveSourceId(tapdataEvent, recordEvent);
            if (tableId == null || tableId.isEmpty()) {
                tableId = "unknown_table";
            }
            String contextKey = buildContextKey(sourceId, tableId);
            String targetTableName = buildTargetTableName(sourceId, tableId);
            PerSourceContext context = getOrCreateContext(contextKey, targetTableName);

            // ========== 步骤2: 将TapdataEvent添加到Context缓冲区 ==========
            synchronized (context.getCommitLock()) {
                context.addEvent(tapdataEvent);
                // 达到批次大小时触发刷写
                if (context.getBatchBuffer().size() >= context.getBatchSize()) {
                    flushContext(context);
                }
            }

            // ========== 步骤3: 透传事件到下游 ==========
            consumer.accept(tapdataEvent, null);

        } catch (Exception e) {
            // 异常处理: 记录错误并写入DLQ
            logger.error("处理记录事件失败: {}", e.getMessage(), e);

            if (errorHandler != null) {
                try {
                    Map<String, Object> sourceData = extractRecordData(recordEvent);
                    errorHandler.recordError(sourceData != null ? sourceData : new HashMap<>(), e);

                    // 写入死信队列
                    if (dlqWriter != null && sourceData != null) {
                        try {
                            String tableId = TapEventUtil.getTableId(recordEvent);
                            String sourceId = resolveSourceId(tapdataEvent, recordEvent);
                            String contextKey = buildContextKey(sourceId, tableId);
                            dlqWriter.write(contextKey, buildTargetTableName(sourceId, tableId),
                                    Collections.singletonList(sourceData), e);
                        } catch (Exception dlqError) {
                            logger.warn("写入DLQ失败: {}", dlqError.getMessage());
                        }
                    }
                } catch (Exception handlerError) {
                    logger.warn("记录错误信息失败: {}", handlerError.getMessage());
                }
            }

            // 处理失败时仍然透传事件
            consumer.accept(tapdataEvent, null);
        }
    }

    /**
     * 从记录事件中提取数据
     */
    private Map<String, Object> extractRecordData(TapRecordEvent recordEvent) {
        if (recordEvent == null) {
            return null;
        }

        // 获取记录的 after 数据（INSERT/UPDATE 事件）
        Map<String, Object> after = TapEventUtil.getAfter(recordEvent);
        if (after != null && !after.isEmpty()) {
            return new HashMap<>(after);
        }

        // 获取记录的 before 数据（DELETE 事件）
        Map<String, Object> before = TapEventUtil.getBefore(recordEvent);
        if (before != null && !before.isEmpty()) {
            return new HashMap<>(before);
        }

        return null;
    }



    /**
     * 刷新单个Context的数据到DuckDB
     * 严格保证执行顺序：
     *   1. calculateAffectedBeforeKeys → 数据写入DuckDB之前
     *   2. 当前Context数据 → 写入当前Context对应表
     *   3. calculateAffectedAfterKeys → 数据写入DuckDB之后、宽表更新之前
     *   4. updateWideTable → 最后执行
     * 
     * @param context 要刷写的上下文
     */
    private void flushContext(PerSourceContext context) throws SQLException {
        if (context == null) {
            return;
        }

        // 保留Context内部锁
        synchronized (this) {
            // 提取当前批次的所有TapdataEvent
            List<TapdataEvent> eventsToFlush = context.drainBuffer();
            if (eventsToFlush.isEmpty()) {
                return;
            }

            try {
                // ========== 步骤1: 计算beforeKeys（数据写入DuckDB之前） ==========
                Set<Object> beforeKeys = null;
                if (affectedKeyCalculator != null && !eventsToFlush.isEmpty()) {
                    beforeKeys = affectedKeyCalculator.calculateAffectedBeforeKeys(eventsToFlush, context.getKey());
                }

                // ========== 步骤2: 写入当前Context对应表 ==========
                if (!eventsToFlush.isEmpty()) {
                    // 直接使用 TapdataEvent 进行合并
                    ensureTableExists(context, eventsToFlush);
                    List<Map<String, Object>> merged = SmartMerger.mergeLastWins(eventsToFlush);
                    
                    if (merged != null && !merged.isEmpty()) {
                        DuckDbOperator operator = context.getOperator() != null ? 
                            context.getOperator() : duckDbOperator;
                        if (operator == null) {
                            throw new SQLException("DuckDbOperator not initialized");
                        }
                        
                        operator.upsertBatch(context.getTargetTableName(), merged);
                        logger.debug("刷写 {} 条记录到DuckDB表: {} (原始 {} 条)", 
                            merged.size(), context.getTargetTableName(), eventsToFlush.size());
                    }
                }

                // ========== 步骤3: 计算afterKeys（数据写入DuckDB之后、宽表更新之前） ==========
                Set<Object> afterKeys = null;
                if (affectedKeyCalculator != null && !eventsToFlush.isEmpty()) {
                    afterKeys = affectedKeyCalculator.calculateAffectedAfterKeys(eventsToFlush);
                }

                // ========== 步骤4: 更新宽表（最后执行） ==========
                if (wideTableUpdater != null && beforeKeys != null && afterKeys != null) {
                    List<Map<String, Object>> afterRows = extractAfterRowsFromEvents(eventsToFlush);
                    List<TapdataEvent> events = wideTableUpdater.updateWideTableAsTapdataEvents(
                        beforeKeys, afterKeys, afterRows, mainTableName);
                    logger.info("更新宽表: {} 个事件", events.size());
                }

            } catch (Exception e) {
                logger.error("刷写Context {} 到DuckDB失败: {}", context.getKey(), e.getMessage(), e);
                // 数据回滚到缓冲区
                context.getBatchBuffer().addAll(0, eventsToFlush);
                context.getAccumulatedRecordCount().addAndGet(eventsToFlush.size());
                writeToDlq(context, extractDataFromEvents(eventsToFlush), e);
            }
        }
    }

    /**
     * 从TapdataEvent列表中提取after数据行
     * 
     * @param events TapdataEvent列表
     * @return after数据行列表
     */
    private List<Map<String, Object>> extractAfterRowsFromEvents(List<TapdataEvent> events) {
        List<Map<String, Object>> afterRows = new ArrayList<>();
        
        for (TapdataEvent event : events) {
            TapEvent tapEvent = event.getTapEvent();
            if (tapEvent instanceof TapRecordEvent) {
                Map<String, Object> after = TapEventUtil.getAfter((TapRecordEvent) tapEvent);
                if (after != null && !after.isEmpty()) {
                    afterRows.add(after);
                }
            }
        }
        
        return afterRows;
    }

    /**
     * 从TapdataEvent列表中提取数据用于DLQ写入
     * 
     * @param events TapdataEvent列表
     * @return 数据行列表
     */
    private List<Map<String, Object>> extractDataFromEvents(List<TapdataEvent> events) {
        List<Map<String, Object>> dataRows = new ArrayList<>();
        
        for (TapdataEvent event : events) {
            TapEvent tapEvent = event.getTapEvent();
            if (tapEvent instanceof TapRecordEvent) {
                // 优先取after数据，如果没有则取before数据
                Map<String, Object> after = TapEventUtil.getAfter((TapRecordEvent) tapEvent);
                if (after != null && !after.isEmpty()) {
                    dataRows.add(new HashMap<>(after));
                } else {
                    Map<String, Object> before = TapEventUtil.getBefore((TapRecordEvent) tapEvent);
                    if (before != null && !before.isEmpty()) {
                        dataRows.add(new HashMap<>(before));
                    }
                }
            }
        }
        
        return dataRows;
    }



    private void ensureTableExists(PerSourceContext context, List<TapdataEvent> events) throws SQLException {
        if (context.isTableInitialized() && context.getTargetTableName() != null) {
            return;
        }
        if (events == null || events.isEmpty()) {
            return;
        }

        TapTable tapTable = inferTapTable(events, context.getTargetTableName());
        if (tapTable != null && tapTable.getNameFieldMap() != null && !tapTable.getNameFieldMap().isEmpty()) {
            DuckDbOperator operator = context.getOperator() != null ? context.getOperator() : duckDbOperator;
            if (operator == null) {
                throw new SQLException("DuckDbOperator not initialized");
            }
            boolean isPreview = processorBaseContext.getTaskDto() != null 
                && processorBaseContext.getTaskDto().isPreviewTask();
            operator.createTempTable(tapTable, context.getTargetTableName(), isPreview);
            context.setTableInitialized(true);
            logger.info("Created temp table for context {}: {} (preview: {})", context.getKey(), context.getTargetTableName(), isPreview);
        }
    }

    /**
     * 从 TapdataEvent 数据中推断 TapTable 结构
     */
    private TapTable inferTapTable(List<TapdataEvent> events, String tableName) {
        if (events == null || events.isEmpty()) {
            return null;
        }

        // 找到第一个有数据的 TapRecordEvent
        for (TapdataEvent tapEvent : events) {
            TapEvent tapEventInner = tapEvent.getTapEvent();
            if (tapEventInner instanceof TapRecordEvent recordEvent) {
                Map<String, Object> after = TapEventUtil.getAfter(recordEvent);
                if (after != null && !after.isEmpty()) {
                    // 找到了有数据的事件，用它来推断表结构
                    TapTable tapTable = new TapTable(tableName);

                    for (Map.Entry<String, Object> entry : after.entrySet()) {
                        String fieldName = entry.getKey();
                        Object value = entry.getValue();

                        TapField tapField = new TapField();
                        tapField.name(fieldName);
                        tapField.tapType(inferTapType(value));
                        tapField.dataType(inferDataType(value));

                        tapTable.add(tapField);
                    }

                    return tapTable;
                }
            }
        }

        return null;
    }

    /**
     * 根据值推断 TapType
     */
    private TapType inferTapType(Object value) {
        if (value == null) {
            return createTapString();
        }

        String className = value.getClass().getSimpleName();
        return switch (className) {
            case "Integer", "Int", "Long", "BigInteger", "Float", "Double", "BigDecimal" -> createTapNumber();
            case "Boolean" -> createTapBoolean();
            case "Date", "Timestamp", "DateTime" -> createTapDate();
            default -> createTapString();
        };
    }

    /**
     * 根据值推断数据类型字符串
     */
    private String inferDataType(Object value) {
        if (value == null) {
            return "VARCHAR";
        }

        return switch (value.getClass().getSimpleName()) {
            case "Integer", "Int" -> "INTEGER";
            case "Long", "BigInteger" -> "BIGINT";
            case "Float" -> "FLOAT";
            case "Double", "BigDecimal" -> "DOUBLE";
            case "Boolean" -> "BOOLEAN";
            case "Date", "Timestamp", "DateTime" -> "TIMESTAMP";
            case "byte[]" -> "BLOB";
            default -> "VARCHAR";
        };
    }

    // ==================== TapType 创建辅助方法 ====================

    private TapType createTapString() {
        return new TapString();
    }

    private TapType createTapNumber() {
        return new TapNumber();
    }

    private TapType createTapBoolean() {
        return new TapBoolean();
    }

    private TapType createTapDate() {
        return new TapDate();
    }

    private String resolveSourceId(TapdataEvent tapdataEvent, TapEvent tapEvent) {
        if (tapEvent instanceof TapBaseEvent baseEvent) {
            String associateId = baseEvent.getAssociateId();
            if (associateId != null && !associateId.isBlank()) {
                return associateId;
            }
        }
        String fromNodeId = tapdataEvent != null ? tapdataEvent.getFromNodeId() : null;
        return (fromNodeId != null && !fromNodeId.isBlank()) ? fromNodeId : DEFAULT_SOURCE_ID;
    }

    private String buildContextKey(String sourceId, String tableId) {
        return sourceId + ":" + tableId;
    }

    private String buildTargetTableName(String sourceId, String tableId) {
        return sanitizeIdentifier(sourceId) + "__" + sanitizeIdentifier(tableId);
    }

    private String sanitizeIdentifier(String value) {
        if (value == null || value.isBlank()) {
            return "unknown";
        }
        String sanitized = value.replaceAll("[^A-Za-z0-9_]", "_");
        if (Character.isDigit(sanitized.charAt(0))) {
            return "_" + sanitized;
        }
        return sanitized;
    }

    private PerSourceContext getOrCreateContext(String contextKey, String targetTableName) {
        synchronized (sourceContextLock) {
            PerSourceContext existing = sourceContexts.get(contextKey);
            if (existing != null) {
                sourceAccessOrder.put(contextKey, Boolean.TRUE);
                return existing;
            }
            evictIfNecessary();
            DuckDbOperator contextOperator = createContextOperator();
            PerSourceContext context = new PerSourceContext(contextKey, contextOperator);
            context.setBatchSize(batchSize);
            context.setTargetTableName(targetTableName);
            sourceContexts.put(contextKey, context);
            sourceAccessOrder.put(contextKey, Boolean.TRUE);
            return context;
        }
    }

    private DuckDbOperator createContextOperator() {
        try {
            return new DuckDbOperatorImpl(true, batchSize, commitIntervalMs);
        } catch (SQLException e) {
            logger.warn("Failed to create dedicated DuckDB operator, fallback to shared operator: {}", e.getMessage());
            return duckDbOperator;
        }
    }

    private void flushAllContexts() {
        for (PerSourceContext context : new ArrayList<>(sourceContexts.values())) {
            synchronized (context.getCommitLock()) {
                try {
                    flushContext(context);
                } catch (Exception e) {
                    logger.warn("Failed to flush context {} during close: {}", context.getKey(), e.getMessage(), e);
                }
            }
        }
    }

    private void flushAllContextsSafely() {
        if (sourceContexts.isEmpty()) {
            return;
        }
        try {
            flushAllContexts();
        } catch (Exception e) {
            logger.warn("Scheduled DuckDB context flush failed: {}", e.getMessage(), e);
        }
    }

    private void evictIfNecessary() {
        while (sourceContexts.size() >= maxActiveSources && !sourceAccessOrder.isEmpty()) {
            Iterator<Map.Entry<String, Boolean>> iterator = sourceAccessOrder.entrySet().iterator();
            if (!iterator.hasNext()) {
                return;
            }
            String eldestKey = iterator.next().getKey();
            iterator.remove();
            PerSourceContext evicted = sourceContexts.remove(eldestKey);
            if (evicted != null) {
                try {
                    flushContext(evicted);
                } catch (Exception e) {
                    logger.warn("Failed to flush evicted context {}: {}", eldestKey, e.getMessage(), e);
                } finally {
                    closeContextOperator(evicted);
                }
            }
        }
    }

    private void closeContextOperator(PerSourceContext context) {
        if (context == null || context.getOperator() == null || context.getOperator() == duckDbOperator) {
            return;
        }
        try {
            context.getOperator().close();
        } catch (Exception e) {
            logger.warn("Failed to close context operator for {}: {}", context.getKey(), e.getMessage(), e);
        }
    }

    private void writeToDlq(PerSourceContext context, List<Map<String, Object>> payload, Exception error) {
        if (dlqWriter == null || context == null) {
            return;
        }
        try {
            dlqWriter.write(context.getKey(), context.getTargetTableName(), payload, error);
        } catch (RuntimeException dlqError) {
            logger.warn("Failed to persist DuckDB DLQ record for {}: {}", context.getKey(), dlqError.getMessage(), dlqError);
        }
    }

    @Override
    protected void doClose() {
        super.doClose();
        if (contextFlusher != null) {
            contextFlusher.shutdownNow();
        }
        flushAllContexts();
        for (PerSourceContext context : new ArrayList<>(sourceContexts.values())) {
            closeContextOperator(context);
        }
        sourceContexts.clear();
        synchronized (sourceContextLock) {
            sourceAccessOrder.clear();
        }

        // 关闭 DuckDB 操作器
        if (duckDbOperator != null) {
            try {
                duckDbOperator.close();
                logger.info("DuckDbSqlNode closed successfully");
            } catch (Exception e) {
                logger.warn("Failed to close DuckDbOperator: {}", e.getMessage());
            }
        }
    }

    /**
     * 执行 SQL 查询
     */
    public List<Map<String, Object>> executeQuery(String sql) throws SQLException {
        if (duckDbOperator == null) {
            throw new SQLException("DuckDbOperator not initialized");
        }
        return duckDbOperator.executeQuery(sql);
    }

    /**
     * 执行 SQL 更新
     */
    public int executeUpdate(String sql) throws SQLException {
        if (duckDbOperator == null) {
            throw new SQLException("DuckDbOperator not initialized");
        }
        return duckDbOperator.executeUpdate(sql);
    }

    /**
     * 设置批处理大小
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * 获取当前批处理大小
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * 获取 DuckDB 操作器（用于测试和扩展）
     */
    public DuckDbOperator getDuckDbOperator() {
        return duckDbOperator;
    }
}
