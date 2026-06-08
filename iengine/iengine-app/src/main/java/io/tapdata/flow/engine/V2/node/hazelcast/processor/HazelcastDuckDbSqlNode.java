package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.duckdb.*;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * DuckDB SQL 处理器节点
 * 继承 HazelcastProcessorBaseNode，支持流式处理模式
 * 使用 Arrow 零拷贝写入实现高性能数据处理
 */
public class HazelcastDuckDbSqlNode extends HazelcastProcessorBaseNode {

    private static final Logger logger = LogManager.getLogger(HazelcastDuckDbSqlNode.class);

    /** DuckDB 操作器 */
    private DuckDbOperator duckDbOperator;
    private static final int DEFAULT_BATCH_SIZE = 1000;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private static final int DEFAULT_MAX_ACTIVE_SOURCES = 50;
    private static final int DEFAULT_COMMIT_INTERVAL_MS = 5000;
    private static final String DLQ_COLLECTION = "duckdb_dlq_records";
    private final Map<String, PerSourceContext> sourceContexts = new ConcurrentHashMap<>();
    private final LinkedHashMap<String, Boolean> sourceAccessOrder = new LinkedHashMap<>(16, 0.75f, true);
    private final Object sourceContextLock = new Object();

    /**
     * 核心优化：前置节点 Schema 信息缓存
     *
     * [nodeSchemaCache]Key: 前置节点的唯一标识 (sourceId/nodeId)
     * [tableSchemaCache]Key: 前置节点的唯一标识 (sourceId/nodeId)
     * Value: 该节点的完整 Schema 信息 (NodeSchemaInfo)
     *
     * 包含信息：
     * - tableName (表名)
     * - qualifiedName (完全限定名，全局唯一)
     * - primaryKeys (主键列表)
     * - fieldMap (字段名 → 字段定义映射)
     * - fieldTypeMap (字段名 → 字段类型映射)
     *
     * 性能优势：
     * - 初始化时一次性加载（O(N)，N = 前置节点数量）
     * - 运行时 O(1) 直接访问（0 次额外查询）
     * - 避免每次事件处理时重复查询 TapTableMap
     */
    private final ConcurrentHashMap<String, NodeSchemaInfo> nodeSchemaCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, NodeSchemaInfo> tableSchemaCache = new ConcurrentHashMap<>();

    /**
     * Schema 缓存是否已初始化的标志位
     */
    private volatile boolean schemaCacheInitialized = false;
    private final int maxActiveSources = DEFAULT_MAX_ACTIVE_SOURCES;
    private final int commitIntervalMs = DEFAULT_COMMIT_INTERVAL_MS;
    private ScheduledExecutorService contextFlusher;
    private DlqWriter dlqWriter;

    // Integration helpers for full/CDC flow
    private SyncStageTracker syncStageTracker;
    private ErrorHandler errorHandler;
    // ========== 新增: 实时增量物化视图组件 ==========
    private AffectedKeyCalculator affectedKeyCalculator;
    private WideTableIncrementalUpdater wideTableUpdater;

    private static final int ERROR_THRESHOLD_COUNT = 100;
    private static final double ERROR_THRESHOLD_RATE = 0.01; // 1%
    private static final int OUTPUT_BATCH_SIZE = 1000;
    private static final int QUERY_TIMEOUT_MS = 5000;

    // SQL configuration
    private String querySql = "SELECT * FROM %s";
    private String wideTableName;
    /** DuckDB 数据库文件路径（支持节点级、全局级配置） */
    private String dbPath;
    private boolean executeQueryOnFullSyncComplete = false;

    // ========== 新增: 实时增量物化视图配置 ==========
    private String wideTablePrimaryKey;
    private boolean outputChangelogEnabled = false;
    private String mainTableName;
    private String mainTablePrimaryKey;
    private List<FromTableConfig> fromTables = new ArrayList<>();
    private Map<String, String> customJoinQueries = new HashMap<>();



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
    private volatile BiConsumer<TapdataEvent, ProcessResult> currentConsumer;

    public HazelcastDuckDbSqlNode(ProcessorBaseContext processorBaseContext) {
        super(processorBaseContext);
    }

    @Override
    protected void doInit(@NotNull Context context) throws TapCodeException {
        super.doInit(context);

        DuckLakeConfig duckLakeConfig = DuckLakeConfig.disabled();
        this.dbPath = null;

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

                // 读取宽表名
                if (nodeConfig.getWideTableName() != null) {
                    this.wideTableName = nodeConfig.getWideTableName();
                }

                // 读取批大小
                if (nodeConfig.getBatchSize() != null) {
                    this.batchSize = nodeConfig.getBatchSize();
                }

                // 读取是否在全量结束后执行查询
                if (nodeConfig.getExecuteQueryOnFullSyncComplete() != null) {
                    this.executeQueryOnFullSyncComplete = nodeConfig.getExecuteQueryOnFullSyncComplete();
                }

                // 读取 DuckLake 配置（支持三级优先级：节点 > 全局 > 默认值）
                Boolean nodeDuckLakeEnabled = nodeConfig.getDuckLakeEnabled();
                if (Boolean.TRUE.equals(nodeDuckLakeEnabled) || 
                    (nodeDuckLakeEnabled == null && DuckDbSqlConfig.isDuckLakeEnabled())) {
                    
                    // 使用节点配置或回退到全局配置
                    String storageType = nodeConfig.getDuckLakeStorageType() != null ? 
                        nodeConfig.getDuckLakeStorageType() : DuckDbSqlConfig.getDuckLakeStorageType();
                    String storagePath = nodeConfig.getDuckLakeStoragePath() != null ? 
                        nodeConfig.getDuckLakeStoragePath() : DuckDbSqlConfig.getDuckLakeStoragePath();
                    String metadataUrl = nodeConfig.getDuckLakeMetadataDbUrl() != null ? 
                        nodeConfig.getDuckLakeMetadataDbUrl() : DuckDbSqlConfig.getDuckLakeMetadataDbUrl();
                    
                    duckLakeConfig = new io.tapdata.flow.engine.V2.node.duckdb.DuckLakeConfig(
                            true, storageType, storagePath, metadataUrl
                    );
                }
                
                // 读取 dbPath 配置（三级优先级：节点 > 全局 > 默认null）
                if (nodeConfig.getDbPath() != null && !nodeConfig.getDbPath().trim().isEmpty()) {
                    dbPath = nodeConfig.getDbPath().trim();
                    logger.info("Using node-level dbPath: {}", dbPath);
                } else if (DuckDbSqlConfig.getDbPath() != null && !DuckDbSqlConfig.getDbPath().trim().isEmpty()) {
                    dbPath = DuckDbSqlConfig.getDbPath().trim();
                    logger.info("Using global config dbPath: {}", dbPath);
                }

                // ========== 新增: 追加 nodeId 到 dbPath 中 ==========
                if (dbPath != null && !dbPath.isEmpty() && processorBaseContext != null) {
                    com.tapdata.tm.commons.dag.Node<?> node = processorBaseContext.getNode();
                    if (node != null && node.getId() != null && !node.getId().isEmpty()) {
                        dbPath = buildDbPathWithNodeId(dbPath, node.getId());
                        logger.info("Final dbPath with nodeId: {}", dbPath);
                    }
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
                                try {
                                    String preNodeId = (String) ft.getClass().getMethod("getPreNodeId").invoke(ft);
                                    String tableNameInSql = (String) ft.getClass().getMethod("getTableNameInSql").invoke(ft);
                                    return new FromTableConfig(preNodeId, tableNameInSql);
                                } catch (Exception e) {
                                    logger.warn("Failed to convert FromTableConfig: {}", e.getMessage());
                                    return null;
                                }
                            })
                            .filter(java.util.Objects::nonNull)
                            .collect(java.util.stream.Collectors.toList());
                }
                if (nodeConfig.getCustomJoinQueries() != null) {
                    this.customJoinQueries = new HashMap<>(nodeConfig.getCustomJoinQueries());
                }

                // ========== 核心优化：初始化 Schema 缓存 ==========
                initSchemaCache();

                // ========== QuerySql 统一处理 ==========
                if (this.querySql != null) {
                    String normalizedSql = QuerySqlProcessor.normalize(this.querySql);
                    QuerySqlProcessor.ValidationResult result = 
                        QuerySqlProcessor.validate(normalizedSql, this.nodeSchemaCache);
                    
                    if (!result.isValid) {
                        throw new IllegalArgumentException("QuerySql validation failed: " + result.errorMessage);
                    }
                    
                    this.querySql = normalizedSql;  // 直接替换原值
                    logger.info("QuerySql normalized and validated successfully");
                }

                // ========== 新增: 确定主表信息和默认值 ==========
                resolveMainTableInfo();

                logger.info("DuckDbSqlNode loaded config: querySql={}, wideTableName={}, batchSize={}, executeQueryOnFullSyncComplete={}, duckLake={}, materializedView={}",
                        querySql, wideTableName, batchSize, executeQueryOnFullSyncComplete, duckLakeConfig.isEnabled(),
                        wideTablePrimaryKey != null ? "enabled" : "disabled");
            }
        } catch (Exception e) {
            logger.error("Failed to load DuckDbSqlNode config, using defaults: {}", e.getMessage());
            throw e;
        }

        // 初始化 DuckDB 操作器（支持内存/文件持久化模式）
        try {
            duckDbOperator = new DuckDbOperatorImpl(dbPath, false, batchSize, 5000, duckLakeConfig);
            contextFlusher = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread thread = new Thread(r, "duckdb-multi-source-flusher");
                thread.setDaemon(true);
                return thread;
            });
            contextFlusher.scheduleAtFixedRate(this::flushAllContextsSafely, commitIntervalMs, commitIntervalMs, TimeUnit.MILLISECONDS);
            dlqWriter = new DlqWriter(clientMongoOperator, DLQ_COLLECTION);

            // 初始化全量/CDC集成辅助组件
            syncStageTracker = new SyncStageTracker();
            errorHandler = new ErrorHandler(ERROR_THRESHOLD_COUNT, ERROR_THRESHOLD_RATE);

            // ========== 新增: 初始化实时增量物化视图组件 ==========
            if (wideTablePrimaryKey != null && !wideTablePrimaryKey.isEmpty()) {
                affectedKeyCalculator = new AffectedKeyCalculator(
                        wideTablePrimaryKey,
                        mainTableName,
                        mainTablePrimaryKey,
                        fromTables,
                        customJoinQueries,
                        duckDbOperator,
                        tableSchemaCache,
                        this.querySql
                );

                logger.info("AffectedKeyCalculator initialized with {} schema(s), querySql length={}",
                           nodeSchemaCache.size(),
                           this.querySql.length());

                // ========== 核心功能：SQL 表别名解析与替换 ==========
                resolveSqlTableAliases();

                // 获取宽表的 NodeSchemaInfo
                NodeSchemaInfo wideTableSchemaInfo = null;
                if (wideTableName != null && !wideTableName.isEmpty()) {
                    wideTableSchemaInfo = tableSchemaCache.get(wideTableName);
                    if (wideTableSchemaInfo == null) {
                        // 尝试从 nodeSchemaCache 中查找
                        for (NodeSchemaInfo schemaInfo : nodeSchemaCache.values()) {
                            if (wideTableName.equals(schemaInfo.getTableName())) {
                                wideTableSchemaInfo = schemaInfo;
                                break;
                            }
                        }
                    }
                    logger.info("Found wide table NodeSchemaInfo: {}", wideTableSchemaInfo != null ? wideTableSchemaInfo.getTableName() : "not found");
                    if (wideTableSchemaInfo == null) {
                        throw new IllegalArgumentException("Wide table name not found: " + wideTableName);
                    }
                } else {
                    throw new IllegalArgumentException("Wide table name not found: " + wideTableName);
                }
                
                wideTableUpdater = new WideTableIncrementalUpdater(
                        wideTableName,
                        wideTablePrimaryKey,
                        querySql,
                        new WithCteSqlGenerator(),
                        duckDbOperator,
                        true, // 是否写宽表
                        wideTableSchemaInfo
                );

                // 注册 Changelog 监听器
                if (outputChangelogEnabled) {
                    wideTableUpdater.addChangelogListener(this::emitEvent);
                }

                logger.info("Materialized view components initialized: affectedKeyCalculator={}, wideTableUpdater={}",
                        affectedKeyCalculator != null, wideTableUpdater != null);
            }

            // Register callback for when all tables enter CDC
            syncStageTracker.setOnAllTablesCdcCallback(v -> {
                try {
                    handleAllTablesCdcTransition();
                } catch (Exception e) {
                    logger.error("Error handling all tables CDC transition: {}", e.getMessage(), e);
                }
            });

            // ========== 核心功能：DuckDB 表生命周期管理： ==========
            manageDuckDbTables();

            logger.info("DuckDbSqlNode initialized with batchSize={}, output batch size={}, error threshold={}% (count: {}), schemaCache={}",
                    batchSize, OUTPUT_BATCH_SIZE, ERROR_THRESHOLD_RATE * 100, ERROR_THRESHOLD_COUNT,
                    nodeSchemaCache.size() > 0 ? "loaded (" + nodeSchemaCache.size() + " nodes)" : "empty");
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

        try {
            // 步骤1: 刷写所有剩余数据
            flushAllContexts();

            // 步骤2: 更新宽表（可选）一次，直接用 insert + querySQL 语句一次性写入
            if (wideTableUpdater != null) {
                try {
                    duckDbOperator.executeInTransaction(this::updateWideTableInFullSyncComplete);
                    logger.info("全量结束后宽表更新完成");
                } catch (Exception e) {
                    logger.error("全量结束后更新宽表失败: {}", e.getMessage(), e);
                }
            }

            // 步骤3: 生成宽表 CDC 事件，直接用 select 查询宽表语句查询出所有结果集拼接 insertEvent
            if (outputChangelogEnabled) {
                try {
                    List<TapdataEvent> wideTableEvents = generateWideTableInsertEvents();
                    if (!wideTableEvents.isEmpty()) {

                        // 发射宽表 CDC 事件到下游
                        for (TapdataEvent event : wideTableEvents) {
                            pendingEvents.offer(event);
                        }

                        logger.info("发射 {} 个宽表 CDC 事件到下游", wideTableEvents.size());
                    }
                } catch (Exception e) {
                    logger.error("生成宽表 CDC 事件失败: {}", e.getMessage(), e);
                }
            }

        } catch (Exception e) {
            logger.error("全量同步完成后处理失败: {}", e.getMessage(), e);
        }
    }

    /**
     * 全量结束后更新宽表，直接用 insert + querySQL 语句一次性写入
     */
    private void updateWideTableInFullSyncComplete() throws SQLException {
        if (querySql == null || querySql.isEmpty()) {
            logger.warn("querySql 为空，跳过宽表更新");
            return;
        }

        // 构建 INSERT INTO ... SELECT ... 语句
        String insertSql = String.format(
            "INSERT INTO %s %s",
            wideTableName,
            querySql
        );

        logger.info("全量结束后执行宽表更新 SQL: {}", insertSql);
        duckDbOperator.executeUpdate(insertSql);
    }

    /**
     * 生成宽表 CDC 事件，直接用 select 查询宽表语句查询出所有结果集拼接 insertEvent
     */
    private List<TapdataEvent> generateWideTableInsertEvents() throws SQLException {
        List<TapdataEvent> events = new ArrayList<>();

        // 直接用 select 查询宽表语句查询出所有结果集
        String selectSql = String.format("SELECT * FROM %s",wideTableName);
        List<Map<String, Object>> rows = duckDbOperator.executeQuery(selectSql);

        if (rows.isEmpty()) {
            logger.warn("宽表中没有数据，跳过生成 CDC 事件");
            return events;
        }

        // 为每一行数据生成 INSERT 类型的 TapdataEvent
        for (Map<String, Object> row : rows) {
            TapInsertRecordEvent insertEvent = TapInsertRecordEvent.create()
                .table(wideTableName)
                .after(row);

            TapdataEvent tapdataEvent = new TapdataEvent();
            tapdataEvent.setTapEvent(insertEvent);
            events.add(tapdataEvent);
        }

        logger.info("从宽表查询到 {} 条数据，生成了 {} 个 INSERT 事件", rows.size(), events.size());
        return events;
    }

    public void setQuerySql(String querySql) {
        DuckDbOperator.ensureSelectQuery(querySql, "DuckDbSqlNode querySql setter");
        this.querySql = querySql;
    }

    public String getWideTableName() {
        return wideTableName;
    }

    public void setWideTableName(String wideTableName) {
        this.wideTableName = wideTableName;
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

        // 步骤2: 发送待处理事件:先清旧队列，保证时序：先发上一轮积压事件，再处理当前输入，避免“旧结果被新事件插队”。
        emitPendingEvents();

        // 步骤3: 空事件检查
        if (tapdataEvent == null) {
            return;
        }

        TapEvent tapEvent = tapdataEvent.getTapEvent();

        // 步骤4: 错误率监控：现在统计的是“进入处理的事件总数”，然后立刻判停。若后移到宽表写完后，分母会变成“处理成功/走到后半段的事件”，错误率会被扭曲。
        if (errorHandler != null) {
            errorHandler.recordEvent();

            // 检查是否超过错误阈值需要停止任务
            if (errorHandler.shouldStopTask()) {
                logger.error("超过错误阈值，任务应停止");
                return;
            }
        }

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
                emitEvent(event);
            }
        }
    }

    private void emitEvent(TapdataEvent event) {
        if (currentConsumer != null) {
            try {
                currentConsumer.accept(event, getProcessResult(TapEventUtil.getTableId(event.getTapEvent())));
                logger.debug("Generated changelog event: {}", event);
            } catch (Exception e) {
                logger.warn("发送事件失败: event={},{}", event, e.getMessage());
            }
        }
    }

    /**
     * 追踪数据事件的同步阶段
     * 记录每个表当前所处的同步阶段(全量/CDC)
     *
     * @param targetTableName Duckdb表名
     * @param tapdataEvent    数据事件
     */
    private void trackSyncStage(String targetTableName, TapdataEvent tapdataEvent) {
        if (syncStageTracker != null && tapdataEvent != null) {
            SyncStage stage = tapdataEvent.getSyncStage();
            TapEvent tapEvent = tapdataEvent.getTapEvent();

            if (tapEvent instanceof TapRecordEvent) {
                if (targetTableName != null) {
                    syncStageTracker.updateTableStageFromEvent(targetTableName, stage);
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
            String tableId = TapEventUtil.getTableId(recordEvent);
            String sourceId = resolveSourceId(tapdataEvent);

            if (tableId == null || tableId.isEmpty()) {
                throw new IllegalArgumentException(
                    "Table ID is missing or empty in record event. " +
                    "Source: " + sourceId + ", " +
                    "Event type: " + (recordEvent != null ? recordEvent.getClass().getSimpleName() : "null") + ", " +
                    "Event: " + recordEvent);
            }

            NodeSchemaInfo schemaInfo = getNodeSchema(sourceId);

            String contextKey;
            String targetTableName;

            if (schemaInfo != null && schemaInfo.isValid()) {
                targetTableName = schemaInfo.getTargetTableName();
                contextKey = sourceId + "|" + schemaInfo.getQualifiedName();

                logger.debug("Using preloaded schema - sourceId: {}, table: {}, qn: {}, targetTable: {}",
                           sourceId, schemaInfo.getTableName(), schemaInfo.getQualifiedName(),
                           targetTableName);
            } else {
                targetTableName = buildTargetTableName(sourceId, tableId);
                contextKey = buildContextKey(sourceId, tableId);

                logger.debug("Using fallback mode - sourceId: {}, tableId: {}, contextKey: {}",
                           sourceId, tableId, contextKey);
            }

            // 步骤5: 同步阶段追踪
            trackSyncStage(targetTableName, tapdataEvent);

            PerSourceContext context = getOrCreateContext(
                contextKey,
                targetTableName,
                sourceId,
                tableId,
                schemaInfo
            );

            if (logger.isTraceEnabled() && context.hasSchema()) {
                NodeSchemaInfo ctxSchema = context.getSchema();

                Map<String, Object> after = TapEventUtil.getAfter(recordEvent);
                if (after != null) {
                    for (String fieldName : after.keySet()) {
                        if (ctxSchema.isPrimaryKey(fieldName)) {
                            logger.trace("Primary key field detected: {} (from table {})",
                                       fieldName, ctxSchema.getTableName());
                        }
                    }

                    for (Map.Entry<String, Object> entry : after.entrySet()) {
                        io.tapdata.entity.schema.type.TapType fieldType =
                            ctxSchema.getFieldType(entry.getKey());
                        if (fieldType != null) {
                            logger.trace("Field type: {} -> {}",
                                       entry.getKey(), fieldType.getClass().getSimpleName());
                        }
                    }
                }
            }

            synchronized (context.getCommitLock()) {
                context.addEvent(tapdataEvent);

                if (context.getBatchBuffer().size() >= context.getBatchSize()) {
                    flushContext(context);
                }
            }

        } catch (Exception e) {
            handleError(tapdataEvent, e);
        }
    }

    /**
     * Unified error handling method with enhanced schema context
     */
    private void handleError(TapdataEvent tapdataEvent, Exception e) {
        TapEvent tapEvent = tapdataEvent != null ? tapdataEvent.getTapEvent() : null;
        assert tapdataEvent != null;
        String sourceId = resolveSourceId(tapdataEvent);

        NodeSchemaInfo schemaInfo = getNodeSchema(sourceId);

        String errorContext = buildErrorContext(tapEvent, sourceId, schemaInfo, e);
        logger.error("Failed to process record event: {}", errorContext, e);

        if (errorHandler != null) {
            try {
                Map<String, Object> sourceData = null;

                if (tapEvent instanceof TapRecordEvent) {
                    sourceData = extractRecordData((TapRecordEvent) tapEvent);

                    if (schemaInfo != null && schemaInfo.isValid()) {
                        enrichSourceDataWithSchema(sourceData, schemaInfo);
                    }
                }

                errorHandler.recordError(sourceData != null ? sourceData : new HashMap<>(), e);

                if (dlqWriter != null && sourceData != null && tapEvent instanceof TapRecordEvent) {
                    try {
                        writeDeadLetterQueue(tapdataEvent, (TapRecordEvent) tapEvent,
                                            sourceId, schemaInfo, sourceData, e);
                    } catch (Exception dlqError) {
                        logger.warn("Failed to write DLQ: {}", dlqError.getMessage());
                    }
                }
            } catch (Exception handlerError) {
                logger.warn("Failed to record error info: {}", handlerError.getMessage());
            }
        }

    }

    /**
     * Build detailed error context for logging
     */
    private String buildErrorContext(TapEvent tapEvent, String sourceId,
                                    NodeSchemaInfo schemaInfo, Exception e) {
        StringBuilder sb = new StringBuilder();
        sb.append(e.getClass().getSimpleName()).append(": ").append(e.getMessage());

        if (sourceId != null) {
            sb.append(", Source: ").append(sourceId);
        }

        if (tapEvent != null) {
            sb.append(", EventType: ").append(tapEvent.getClass().getSimpleName());

            if (tapEvent instanceof TapRecordEvent recordEvent) {
                String tableId = TapEventUtil.getTableId(recordEvent);
                if (tableId != null) {
                    sb.append(", TableId: ").append(tableId);
                }
            }
        }

        if (schemaInfo != null && schemaInfo.isValid()) {
            sb.append(", Schema{");
            sb.append("table=").append(schemaInfo.getTableName());
            sb.append(", qn=").append(schemaInfo.getQualifiedName());
            sb.append(", pkCount=").append(schemaInfo.getPrimaryKeys().size());
            sb.append(", fieldCount=").append(schemaInfo.getFieldCount());
            sb.append("}");
        } else if (sourceId != null) {
            sb.append(", SchemaStatus=invalid_or_missing");
        }

        return sb.toString();
    }

    /**
     * Enrich source data with schema metadata for better debugging
     */
    private void enrichSourceDataWithSchema(Map<String, Object> sourceData,
                                           NodeSchemaInfo schemaInfo) {
        if (sourceData == null || schemaInfo == null || !schemaInfo.isValid()) {
            return;
        }

        sourceData.put("_meta_source_id", schemaInfo.getNodeId());
        sourceData.put("_meta_table_name", schemaInfo.getTableName());
        sourceData.put("_meta_qualified_name", schemaInfo.getQualifiedName());
        sourceData.put("_meta_primary_keys", schemaInfo.getPrimaryKeys());
        sourceData.put("_meta_field_count", schemaInfo.getFieldCount());
        sourceData.put("_meta_initialized_time", schemaInfo.getInitializedTime());

        Map<String, Object> after = (Map<String, Object>) sourceData.get("after");
        if (after != null) {
            Map<String, Object> primaryKeyValues = new LinkedHashMap<>();
            for (String pkField : schemaInfo.getPrimaryKeys()) {
                Object pkValue = after.get(pkField);
                if (pkValue != null) {
                    primaryKeyValues.put(pkField, pkValue);
                }
            }
            if (!primaryKeyValues.isEmpty()) {
                sourceData.put("_meta_primary_key_values", primaryKeyValues);
            }
        }
    }

    /**
     * Write failed event to Dead Letter Queue with schema-aware context
     */
    private void writeDeadLetterQueue(TapdataEvent tapdataEvent, TapRecordEvent recordEvent,
                                     String sourceId, NodeSchemaInfo schemaInfo,
                                     Map<String, Object> sourceData, Exception e) {
        String tableId = TapEventUtil.getTableId(recordEvent);

        String contextKey;
        String targetTableName;

        if (schemaInfo != null && schemaInfo.isValid()) {
            targetTableName = schemaInfo.getTargetTableName();
            contextKey = sourceId + "|" + schemaInfo.getQualifiedName();

            logger.debug("Writing to DLQ with preloaded schema - sourceId: {}, table: {}, qn: {}, targetTable: {}",
                       sourceId, schemaInfo.getTableName(), schemaInfo.getQualifiedName(),
                       targetTableName);
        } else {
            if (tableId == null || tableId.isEmpty()) {
                throw new IllegalArgumentException(
                    "Cannot write to DLQ: Table ID is missing in failed event. " +
                    "Source: " + sourceId + ", " +
                    "This indicates a corrupted event that cannot be properly handled.");
            }

            targetTableName = buildTargetTableName(sourceId, tableId);
            contextKey = buildContextKey(sourceId, tableId);

            logger.debug("Writing to DLQ with fallback mode - sourceId: {}, tableId: {}, contextKey: {}",
                       sourceId, tableId, contextKey);
        }

        dlqWriter.write(contextKey, targetTableName, Collections.singletonList(sourceData), e);
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
     * 分为全量阶段和增量阶段
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

            // 判断同步阶段
            boolean isInitialSync = syncStageTracker != null && syncStageTracker.isTableInInitialSync(context.getTargetTableName());

            try {
                if (isInitialSync) {
                    // ========== 全量阶段 ==========
                    processInitialSyncStage(context, eventsToFlush);
                } else {
                    // ========== 增量阶段 ==========
                    processCdcStage(context, eventsToFlush);
                }

            } catch (Exception e) {
                logger.error("刷写Context {} 到DuckDB失败: {}", context.getKey(), e.getMessage(), e);
                context.getAccumulatedRecordCount().addAndGet(eventsToFlush.size());
                writeToDlq(context, extractDataFromEvents(eventsToFlush), e);
                eventsToFlush.clear();
            }
        }
    }

    /**
     * 处理全量阶段
     * 职责：只负责数据合并和写入 DuckDB，不包含宽表更新逻辑
     *
     * 流程：
     * 1. 使用 SmartMerger 合并事件（第二步、第三步）
     * 2. ensureTableExists()
     * 3. DuckDB 事务开启（可选）
     * 4. Before 的所有数据全部执行 delete 操作
     * 5. After 的数据执行 Arrow 批量写入
     *
     * 注意：宽表更新由 handleAllTablesCdcTransition 统一处理
     */
    private void processInitialSyncStage(PerSourceContext context, List<TapdataEvent> eventsToFlush)
        throws SQLException, java.io.IOException {

        if (eventsToFlush == null || eventsToFlush.isEmpty()) {
            return;
        }

        // 步骤2: 确保表存在
//        ensureTableExists(context, eventsToFlush);

        // 获取 DuckDbOperator
        DuckDbOperator operator = getOperatorForContext(context);

        // 步骤2.5: 归一化所有事件值（类型归一化）
//        normalizeEvents(eventsToFlush);

        // 步骤3: DuckDB 事务开启（可选）
        operator.executeInTransaction(() -> {
            // 步骤4: 初始化阶段全部按 insert 直接批量写入
            operator.insertBatch(context.getSchema(), eventsToFlush);

            eventsToFlush.clear();
        });

        logger.debug("全量阶段直接插入 {} 条事件到DuckDB表: {}", eventsToFlush.size(), context.getTargetTableName());
    }

    /**
     * 获取 Context 对应的 DuckDbOperator
     * @param context PerSourceContext
     * @return DuckDbOperator 实例
     * @throws SQLException 如果 Operator 未初始化
     */
    private DuckDbOperator getOperatorForContext(PerSourceContext context) throws SQLException {
        DuckDbOperator operator = context.getOperator() != null ? context.getOperator() : duckDbOperator;
        if (operator == null) {
            throw new SQLException("DuckDbOperator not initialized");
        }
        return operator;
    }

    /**
     * 处理增量阶段
     * 职责：完整的数据写入 + 宽表更新 + CDC 事件生成流程
     *
     * 流程：
     * 1. 使用 SmartMerger 合并事件（第二步、第三步、第四步都用）
     * 2. 计算 beforeKeys（数据写入 DuckDB 之前）
     * 3. ensureTableExists()
     * 4. DuckDB 事务开启（可选）
     * 5. Before 的所有数据全部执行 delete 操作
     * 6. After 的数据执行 Arrow 批量写入
     * 7. 计算 afterKeys（数据写入 DuckDB 之后、宽表更新之前）
     * 8. 更新宽表（可选）
     * 9. 生成宽表 CDC 事件，触发 ChangelogListener，发射到下游
     */
    private void processCdcStage(PerSourceContext context, List<TapdataEvent> eventsToFlush)
        throws SQLException, java.io.IOException {

        if (eventsToFlush == null || eventsToFlush.isEmpty()) {
            return;
        }

        // 步骤1: 使用 SmartMerger 合并事件（第二步、第三步、第四步都用）
        List<SmartMerger.MergedRecord> mergedRecords = SmartMerger.mergeEventsSmart(eventsToFlush, context.getTargetTableName(), context.getSchema());

        // 步骤1.5: 归一化所有 before/after 行值（类型归一化）
//        normalizeMergedRecords(mergedRecords);

        // 步骤2: 确保表存在
//        ensureTableExists(context, eventsToFlush);

        // 获取 DuckDbOperator
        DuckDbOperator operator = getOperatorForContext(context);

        // 步骤3: 计算 beforeKeys（数据写入 DuckDB 之前）
        Set<Object> beforeKeys;
        if (affectedKeyCalculator != null && !mergedRecords.isEmpty()) {
            beforeKeys = affectedKeyCalculator.calculateAffectedBeforeKeys(mergedRecords, context.getTargetTableName());
        } else {
            beforeKeys = null;
        }

        // 步骤4-6: DuckDB 事务开启，写入数据
//        operator.
//                executeInTransaction(() -> {
            // 步骤5: Before 的所有数据全部执行 delete 操作
            deleteBeforeData(operator, context.getTargetTableName(), mergedRecords, context.getSchema());

            // 步骤6: After 的数据执行 Arrow 批量写入
            writeAfterData(operator, context.getSchema(), mergedRecords);

            // 步骤7: 计算 afterKeys（数据写入 DuckDB 之后、宽表更新之前）
            // 返回 AffectedKeysResult 包含 PKs、查询结果和 afterRows，避免重复计算
            AffectedKeyCalculator.AffectedKeysResult afterKeysResult = null;
            if (affectedKeyCalculator != null && !mergedRecords.isEmpty()) {
                afterKeysResult = affectedKeyCalculator.calculateAffectedAfterKeys(mergedRecords, context.getTargetTableName());
            }

            // 步骤8: 更新宽表（可选），发送宽表事件到下游
            // 传递 AffectedKeysResult，避免重复查询和计算
            updateWideTable(context.getTargetTableName(), beforeKeys, afterKeysResult, mergedRecords);

//        });

        logger.debug("增量阶段刷写 {} 条记录到DuckDB表: {} (原始 {} 条)",
            mergedRecords.size(), context.getTargetTableName(), eventsToFlush.size());
    }

    /**
     * 更新宽表（可选）
     *
     * <p>Refactored per 2026-06-07 design: now consumes {@link SmartMerger.MergedRecord}
     * directly instead of re-traversing original events.</p>
     * <p>Optimized per 2026-06-07: accepts AffectedKeysResult to avoid redundant computation.</p>
     *
     * @param targetTableName
     * @param beforeKeys        写入前的主键集合
     * @param afterKeysResult   写入后的主键计算结果（包含PKs、查询结果、afterRows）
     * @param mergedRecords    合并后的记录列表（备用，优先使用 afterKeysResult 中的数据）
     */
    private void updateWideTable(String targetTableName, 
                                 Set<Object> beforeKeys, 
                                 AffectedKeyCalculator.AffectedKeysResult afterKeysResult,
                                 List<SmartMerger.MergedRecord> mergedRecords) {
        if (wideTableUpdater == null || beforeKeys == null || afterKeysResult == null) {
            return;
        }

        try {
            // 使用 AffectedKeysResult 中的预计算数据，避免重复查询
            Set<Object> afterKeys = afterKeysResult.getWideTablePks();
            List<Map<String, Object>> afterRows = afterKeysResult.getAfterRows();
            List<Map<String, Object>> wideTableQueryResults = afterKeysResult.getWideTableQueryResults();
            
            // 传递预计算的查询结果，避免重复执行 WITH CTE 查询
            List<TapdataEvent> wideTableEvents = wideTableUpdater.updateWideTableAsTapdataEvents(
                beforeKeys, afterKeys, wideTableQueryResults, afterRows, targetTableName);
            logger.info("增量阶段更新宽表: {} 个事件", wideTableEvents.size());
        } catch (Exception e) {
            logger.error("更新宽表失败: {}", e.getMessage(), e);
        }
    }

    /**
     * 删除 Before 数据（执行 DELETE 操作）
     *
     * <p>Refactored per 2026-06-07 design: now reads PKs directly from
     * {@link SmartMerger.MergedRecord#getBeforeRows()}, avoiding re-traversal
     * of original operations.</p>
     *
     * @param operator DuckDB 操作器
     * @param tableName 目标表名
     * @param mergedRecords 合并后的记录列表
     * @param schema 表的 schema 信息（用于获取真实主键）
     * @throws SQLException 如果数据库操作失败
     */
    private void deleteBeforeData(DuckDbOperator operator, String tableName,
                                   List<SmartMerger.MergedRecord> mergedRecords,
                                   NodeSchemaInfo schema) throws SQLException {
        List<Object> beforePks = new ArrayList<>(mergedRecords.stream().map(SmartMerger.MergedRecord::getMainTableBeforePks).filter(Objects::nonNull).flatMap(Collection::stream).toList());

        if (!beforePks.isEmpty()) {
            logger.debug("Deleting {} before rows from table '{}'", beforePks.size(), tableName);
            operator.deleteByIds(beforePks, schema);
        }
    }

    /**
     * 写入 After 数据（执行 Arrow 批量写入）
     *
     * <p>Refactored per 2026-06-07 design: now reads rows directly from
     * {@link SmartMerger.MergedRecord#getAfterRows()}, avoiding re-traversal
     * of original operations.</p>
     *
     * @param operator    DuckDB 操作器
     * @param schemaInfo 预加载的Schema信息
     * @param mergedRecords 合并后的记录列表
     * @throws SQLException 如果数据库操作失败
     * @throws java.io.IOException 如果写入失败
     */
    private void writeAfterData(DuckDbOperator operator, NodeSchemaInfo schemaInfo,
                                List<SmartMerger.MergedRecord> mergedRecords) throws SQLException, java.io.IOException {
        List<Map<String, Object>> allAfterRows = new ArrayList<>();
        for (SmartMerger.MergedRecord record : mergedRecords) {
            allAfterRows.addAll(record.getAfterRows());
        }
        if (!allAfterRows.isEmpty()) {
            operator.writeBatch(allAfterRows, schemaInfo);
        }
    }

    /**
     * @deprecated 已迁移至 {@link DuckDbOperator#deleteByIds}。
     *             此方法将在未来版本中移除。
     */
    @Deprecated
    private void buildDeleteSql() {
        throw new UnsupportedOperationException("Use DuckDbOperator.deleteByIds() instead");
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

    // ========== Row Normalizer (per 2026-06-07 design) ==========

    private static final com.fasterxml.jackson.databind.ObjectMapper NORMALIZER_MAPPER =
            new com.fasterxml.jackson.databind.ObjectMapper();

    /**
     * Resolve data source ID from event
     *
     * <p>Priority: associateId > fromNodeId</p>
     *
     * <p>Strict mode: If source ID cannot be resolved, throws exception immediately.
     * This ensures all events have a valid source identifier for proper routing and debugging.</p>
     *
     * @param tapdataEvent Tapdata event wrapper
     * @return Resolved source ID (never null)
     * @throws IllegalArgumentException if source ID cannot be resolved
     */
    private String resolveSourceId(TapdataEvent tapdataEvent) {
        return tapdataEvent.getNodeIds().get(0);
    }

    // ========== 核心优化：Schema 预加载系统 ==========

    /**
     * 初始化前置节点 Schema 信息缓存
     *
     * <p>在 doInit() 阶段调用，一次性加载所有前置节点的完整 Schema 信息，
     * 包括：tableName、qualifiedName、主键、字段名、字段类型等。</p>
     *
     * <p>性能优势：</p>
     * <ul>
     *   <li>初始化时：O(N) 次查询，N = 前置节点数量</li>
     *   <li>运行时：O(1) 直接访问，0 次额外查询</li>
     *   <li>避免每次事件处理时重复查询 TapTableMap</li>
     * </ul>
     *
     * <p>数据源：从 nodeConfig.preNodeTapTables（List&lt;TapTableDto&gt;）获取，
     * 由 TM 侧在 mergeSchema() 时填充。</p>
     */
    private void initSchemaCache() {
        if (schemaCacheInitialized) {
            return;
        }

        try {
            // 从 nodeConfig 读取 preNodeTapTables（TM 侧已在 mergeSchema() 中填充）
            com.tapdata.tm.commons.dag.process.DuckDbSqlNode nodeConfig =
                    (com.tapdata.tm.commons.dag.process.DuckDbSqlNode) getNode();

            if (nodeConfig != null && CollectionUtils.isNotEmpty(nodeConfig.getPreNodeTapTables())) {
                logger.info("Initializing schema cache from nodeConfig, total {} TapTableDtos",
                           nodeConfig.getPreNodeTapTables().size());

                // 使用 IengineSchemaConverter 进行转换
                io.tapdata.flow.engine.V2.node.duckdb.converter.IengineSchemaConverter converter =
                        io.tapdata.flow.engine.V2.node.duckdb.converter.IengineSchemaConverter.getInstance();
                Map<String, NodeSchemaInfo> schemaInfoMap = converter.convert(nodeConfig.getPreNodeTapTables());

                // 填充缓存
                int successCount = 0;
                for (Map.Entry<String, NodeSchemaInfo> entry : schemaInfoMap.entrySet()) {
                    NodeSchemaInfo schemaInfo = entry.getValue();
                    if (schemaInfo.isValid()) {
                        nodeSchemaCache.put(entry.getKey(), schemaInfo);
                        tableSchemaCache.put(schemaInfo.getTableName(), schemaInfo);
                        successCount++;
                        logger.debug("Schema cached from nodeConfig: {} -> {}", entry.getKey(), schemaInfo.getQualifiedName());
                    }
                }

                schemaCacheInitialized = true;
                logger.info("Schema cache initialized from nodeConfig: {}/{}",
                           successCount, nodeConfig.getPreNodeTapTables().size());
                return;
            }

            throw new IllegalStateException("No schemas found for schema cache initialization: preNodeTapTables is empty");

        } catch (Exception e) {
            logger.error("Failed to initialize node schema cache: {}", e.getMessage(), e);
            throw new IllegalStateException("Failed to initialize node schema cache", e);
        }
    }


    // ========== 结束：Schema 预加载系统 ==========

    /**
     * 根据前置节点 ID 快速获取 Schema 信息（O(1)）
     */
    public NodeSchemaInfo getNodeSchema(String sourceId) {
        if (StringUtils.isBlank(sourceId)) {
            return null;
        }

        return nodeSchemaCache.get(sourceId);
    }

    /**
     * 检查 Schema 缓存是否已初始化
     */
    public boolean isSchemaCacheInitialized() {
        return schemaCacheInitialized;
    }

    /**
     * DuckDB 表生命周期管理
     *
     * <p>在初始化阶段执行，确保所有前置节点对应的 DuckDB 表都已正确创建。
     * 根据任务重置标识（shouldRecreate）决定是否删除并重建表。</p>
     *
     * <h3>执行流程：</h3>
     * <ol>
     *   <li>处理主表（mainTable）：如果配置了主表，确保其目标表已创建</li>
     *   <li>处理关联表（fromTables）：遍历所有前置节点，创建/重建对应的目标表</li>
     *   <li>使用 Task 2 实现的 ensureTableExists() 统一接口</li>
     * </ol>
     *
     * <h3>职责分离：</h3>
     * <ul>
     *   <li>DuckDbOperator：提供 ensureTableExists() 公共方法（已实现）</li>
     *   <li>HazelcastDuckDbSqlNode：控制重建策略（本方法）</li>
     * </ul>
     */
    private void manageDuckDbTables() throws SQLException, TapCodeException {
        if (duckDbOperator == null) {
            logger.warn("duckDbOperator is null, skip table management");
            return;
        }

        // 获取任务重置标识（默认为 false，不重建）
        boolean shouldRecreate = shouldRecreateTables();

        logger.info("Starting DuckDB table management. shouldRecreate={}, fromTables count={}",
                   shouldRecreate, fromTables != null ? fromTables.size() : 0);

        int createdCount = 0;
        int recreatedCount = 0;

        // 1. 处理主表（如果配置了 mainTableName）
        if (mainTableName != null && !mainTableName.isBlank()) {
            NodeSchemaInfo mainTableSchema = tableSchemaCache.get(mainTableName);

            if (mainTableSchema != null) {
                String targetTableName = mainTableSchema.getTargetTableName();

                try {
                    duckDbOperator.ensureTableExists(
                        mainTableSchema,
                        shouldRecreate
                    );

                    if (shouldRecreate) {
                        recreatedCount++;
                    } else {
                        createdCount++;
                    }

                    logger.info("Main table managed: {} -> {} (recreate={})",
                               mainTableName, targetTableName, shouldRecreate);
                } catch (Exception e) {
                    throw new TapCodeException("Failed to manage main table: " + mainTableName, e);
                }
            } else {
                logger.warn("Cannot find schema info for main table: {}", mainTableName);
            }
        }

        // 2. 处理关联表（fromTables）
        if (fromTables != null && !fromTables.isEmpty()) {
            for (FromTableConfig fromTable : fromTables) {
                if (fromTable == null) {
                    continue;
                }

                String preNodeId = fromTable.getPreNodeId();

                if (preNodeId == null || preNodeId.isBlank()) {
                    continue;
                }

                NodeSchemaInfo schemaInfo = getNodeSchema(preNodeId);

                if (schemaInfo == null) {
                    logger.warn("Cannot find schema info for preNodeId={}, skipping table creation", preNodeId);
                    continue;
                }

                String targetTableName = schemaInfo.getTargetTableName();

                try {
                    duckDbOperator.ensureTableExists(
                        schemaInfo,
                        shouldRecreate
                    );

                    if (shouldRecreate) {
                        recreatedCount++;
                    } else {
                        createdCount++;
                    }

                    logger.debug("From table managed: {} -> {} (recreate={})",
                                preNodeId, targetTableName, shouldRecreate);
                } catch (Exception e) {
                    throw new TapCodeException(
                        "Failed to manage table for predecessor node: " + preNodeId, e);
                }
            }
        }

        // 3. 处理宽表（如果配置了 wideTableName）
        if (wideTableName != null && !wideTableName.isBlank()) {
            try {
                boolean wideTableManaged = manageWideTable(shouldRecreate);
                if (wideTableManaged) {
                    if (shouldRecreate) {
                        recreatedCount++;
                    } else {
                        createdCount++;
                    }
                }
            } catch (Exception e) {
                throw new TapCodeException("Failed to manage wide table: " + wideTableName, e);
            }
        }

        logger.info("DuckDB table management completed. Created={}, Recreated={}, Total={}",
                   createdCount, recreatedCount, createdCount + recreatedCount);
    }

    /**
     * 判断是否需要重建表
     *
     * <p>根据业务规则判断，当前实现：</p>
     * <ul>
     *   <li>默认返回 false（不重建）</li>
     *   <li>未来可根据任务状态、配置参数等扩展</li>
     * </ul>
     *
     * @return true 表示需要删除并重建表，false 表示仅创建（如果不存在）
     */
    private boolean shouldRecreateTables() {
        // TODO: 根据实际业务需求实现
        // 可能的判断依据：
        // - 任务配置中的 rebuild 标识
        // - 任务重置/重启状态
        // - Schema 变更检测
        return true;
    }

    /**
     * 管理宽表创建
     * 
     * @param shouldRecreate 是否需要重建表
     * @return true 表示表被创建/重建，false 表示表已存在且不需要重建
     */
    private boolean manageWideTable(boolean shouldRecreate) throws SQLException {
        // 1. 先检查表是否已存在
        boolean tableExists = isTableExists(wideTableName);
        
        // 如果表已存在且不需要重建，直接返回
        if (tableExists && !shouldRecreate) {
            logger.info("Wide table '{}' already exists, skipping creation", wideTableName);
            return false;
        }

        // 如果需要重建，先删除旧表
        if (tableExists && shouldRecreate) {
            logger.info("Dropping existing wide table '{}' for recreation", wideTableName);
            duckDbOperator.executeUpdate("DROP TABLE IF EXISTS " + WideTableDdlGenerator.quoteIdentifier(wideTableName));
        }

        // 2. 创建宽表
        logger.info("Creating wide table '{}'...", wideTableName);
        
        // 尝试多种创建策略
        boolean created = false;
        
        // 策略1：优先使用 NodeSchemaInfo（如果有）
        NodeSchemaInfo wideTableSchemaInfo = tableSchemaCache.get(wideTableName);
        if (wideTableSchemaInfo == null) {
            // 如果在 tableSchemaCache 找不到，尝试在 nodeSchemaCache 中查找
            for (NodeSchemaInfo schemaInfo : nodeSchemaCache.values()) {
                if (wideTableName.equals(schemaInfo.getTableName())) {
                    wideTableSchemaInfo = schemaInfo;
                    break;
                }
            }
        }
        
        if (wideTableSchemaInfo != null && wideTableSchemaInfo.getFieldMap() != null && !wideTableSchemaInfo.getFieldMap().isEmpty()) {
            logger.info("Creating wide table using NodeSchemaInfo: {}", wideTableSchemaInfo.getTableName());
            String createDdl = WideTableDdlGenerator.generateCreateTableDdl(wideTableSchemaInfo, wideTablePrimaryKey);
            duckDbOperator.executeUpdate(createDdl);
            logger.info("Successfully created wide table '{}' from NodeSchemaInfo", wideTableName);
            created = true;
        } else {
            // 策略2：尝试 CREATE TABLE ... AS SELECT ... WHERE 1=0
            logger.info("Creating wide table using CREATE TABLE AS SELECT...");
            created = createTableUsingAsSelect(wideTableName, querySql);
            
            // 策略3：降级到传统字段列表方式
            if (!created) {
                logger.warn("CREATE TABLE AS SELECT failed, falling back to traditional DDL");
                createTableUsingFieldList(wideTableName, wideTablePrimaryKey);
                created = true;
            }
        }

        if (created) {
            logger.info("Successfully created wide table '{}'", wideTableName);
        }
        
        return created;
    }

    /**
     * 检查表是否已存在
     */
    private boolean isTableExists(String tableName) throws SQLException {
        try {
            // DuckDB 查询系统表检查表是否存在
            String checkSql = String.format(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s'",
                tableName.toLowerCase()
            );
            
            List<Map<String, Object>> results = duckDbOperator.executeQuery(checkSql);
            
            if (results != null && !results.isEmpty()) {
                Object countObj = results.get(0).values().iterator().next();
                if (countObj instanceof Number) {
                    return ((Number) countObj).intValue() > 0;
                }
            }
            
            return false;
        } catch (Exception e) {
            logger.warn("Failed to check if table '{}' exists: {}", tableName, e.getMessage());
            return false;
        }
    }

    /**
     * 使用 CREATE TABLE ... AS SELECT ... WHERE 1=0 语法创建表
     */
    private boolean createTableUsingAsSelect(String tableName, String querySql) {
        try {
            String createDdl = WideTableDdlGenerator.generateCreateTableAsSelect(tableName, querySql);
            duckDbOperator.executeUpdate(createDdl);
            logger.info("Successfully created table '{}' using CREATE TABLE AS SELECT", tableName);
            return true;
        } catch (Exception e) {
            logger.warn("CREATE TABLE AS SELECT failed for table '{}': {}", tableName, e.getMessage());
            return false;
        }
    }

    /**
     * 使用传统字段列表方式创建表（降级方案）
     */
    private void createTableUsingFieldList(String tableName, String primaryKey) throws SQLException {
        List<String> selectFields = WideTableDdlGenerator.extractSelectFields(querySql);
        
        if (selectFields.isEmpty()) {
            throw new SQLException("Cannot extract fields from querySql");
        }
        
        String createDdl = WideTableDdlGenerator.generateCreateTableDdl(tableName, selectFields, primaryKey);
        duckDbOperator.executeUpdate(createDdl);
        
        logger.info("Successfully created table '{}' using traditional DDL with {} fields", 
                   tableName, selectFields.size());
    }

    /**
     * SQL 表别名解析与替换
     *
     * <p>在初始化阶段一次性完成，将 querySql 中的表别名（tableNameInSql）替换为
     * 实际的目标表名（targetTableName），确保 DuckDB 中写入的表名任务级唯一。</p>
     *
     * <h3>核心算法：</h3>
     * <ol>
     *   <li>遍历 fromTables 配置，获取每个前置节点的 preNodeId 和 tableNameInSql</li>
     *   <li>通过 preNodeId 从 nodeSchemaCache 查找对应的 NodeSchemaInfo</li>
     *   <li>调用 getTargetTableName() 获取实际目标表名（格式：sourceId__tableName）</li>
     *   <li>使用正则表达式 \b（单词边界）检测替换，避免部分匹配</li>
     * </ol>
     *
     * <h3>性能优势：</h3>
     * <ul>
     *   <li>初始化时一次性解析，运行时 0 开销</li>
     *   <li>使用 Pattern.compile() 预编译正则，提升替换效率</li>
     *   <li>支持复杂 JOIN、子查询、CTE 等场景</li>
     * </ul>
     */
    private void resolveSqlTableAliases() {
        if (fromTables == null || fromTables.isEmpty()) {
            logger.debug("No fromTables configured, skip SQL alias resolution");
            return;
        }

        if (querySql == null || querySql.isBlank()) {
            logger.warn("querySql is blank, cannot perform alias resolution");
            return;
        }

        String originalSql = this.querySql;
        Map<String, String> aliasToTargetMap = new LinkedHashMap<>();

        for (FromTableConfig fromTable : fromTables) {
            if (fromTable == null) {
                continue;
            }

            String preNodeId = fromTable.getPreNodeId();
            String tableNameInSql = fromTable.getTableNameInSql();

            if (preNodeId == null || preNodeId.isBlank()) {
                logger.warn("FromTableConfig has blank preNodeId, skipping");
                continue;
            }

            if (tableNameInSql == null || tableNameInSql.isBlank()) {
                logger.warn("FromTableConfig has blank tableNameInSql for preNodeId={}, skipping", preNodeId);
                continue;
            }

            // 通过 preNodeId 查找 NodeSchemaInfo（支持 sourceId 和 nodeId 两种查找方式）
            NodeSchemaInfo schemaInfo = getNodeSchema(preNodeId);

            if (schemaInfo == null) {
                logger.error("Cannot find NodeSchemaInfo for preNodeId={}, " +
                           "SQL alias resolution may be incomplete. " +
                           "Available nodes: {}",
                           preNodeId, nodeSchemaCache.keySet());
                throw new TapCodeException("Failed to find schema info for predecessor node: " + preNodeId);
            }

            // 获取目标表名（格式：sourceId__tableName）
            String targetTableName = schemaInfo.getTargetTableName();

            logger.info("Resolved table alias: '{}' -> '{}' (preNodeId={})",
                       tableNameInSql, targetTableName, preNodeId);

            aliasToTargetMap.put(tableNameInSql, targetTableName);
        }

        if (aliasToTargetMap.isEmpty()) {
            logger.debug("No valid aliases to resolve in querySql");
            return;
        }

        // 使用正则边界检测进行替换
        this.querySql = replaceWithBoundaryDetection(originalSql, aliasToTargetMap);

        logger.info("SQL alias resolution completed. Original: {} -> Resolved: {}",originalSql,this.querySql);
    }

    /**
     * 使用正则边界检测替换 SQL 中的表别名
     *
     * @param sql 原始 SQL 语句
     * @param aliasMap 别名映射（alias → targetTableName）
     * @return 替换后的 SQL 语句
     */
    private String replaceWithBoundaryDetection(String sql, Map<String, String> aliasMap) {
        String currentSql = sql;

        for (Map.Entry<String, String> entry : aliasMap.entrySet()) {
            String alias = entry.getKey();
            String targetTableName = entry.getValue();

            // 使用 \b 单词边界确保只替换完整的标识符，避免部分匹配
            java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
                "\\b" + java.util.regex.Pattern.quote(alias) + "\\b"
            );

            java.util.regex.Matcher matcher = pattern.matcher(currentSql);
            StringBuffer sb = new StringBuffer();

            while (matcher.find()) {
                matcher.appendReplacement(sb, java.util.regex.Matcher.quoteReplacement(targetTableName));
            }
            matcher.appendTail(sb);

            currentSql = sb.toString();
        }

        return currentSql;
    }

    /**
     * 获取已缓存的 Schema 数量
     */
    public int getCachedSchemaCount() {
        return nodeSchemaCache.size();
    }

    // ========== 结束：Schema 预加载系统 ==========

    /**
     * 构建上下文键（用于内存缓存）
     * 格式：sourceId:tableId
     * 例："node_mysql_1__users"
     */
    private String buildContextKey(String sourceId, String tableId) {
        return sourceId + ":" + tableId;
    }

    /**
     * Build target table name (fallback for backward compatibility)
     */
    private String buildTargetTableName(String sourceId, String tableId) {
//        return sanitizeIdentifier(sourceId) + "__" + sanitizeIdentifier(tableId);
        return sanitizeIdentifier(tableId);
    }

    private String sanitizeIdentifier(String value) {
        if (value == null) {
            throw new IllegalArgumentException(
                "Cannot sanitize identifier: value is null. " +
                "This indicates a programming error where a required identifier was not properly initialized.");
        }

        if (value.isBlank()) {
            throw new IllegalArgumentException(
                "Cannot sanitize identifier: value is blank (empty or whitespace only). " +
                "Original value: '" + value + "'");
        }

        String sanitized = value.replaceAll("[^A-Za-z0-9_]", "_");
        if (Character.isDigit(sanitized.charAt(0))) {
            return "_" + sanitized;
        }
        return sanitized;
    }

    private PerSourceContext getOrCreateContext(String contextKey, String targetTableName,
                                                String sourceId, String tableId,
                                                NodeSchemaInfo schemaInfo) {
        synchronized (sourceContextLock) {
            PerSourceContext existing = sourceContexts.get(contextKey);
            if (existing != null) {
                sourceAccessOrder.put(contextKey, Boolean.TRUE);
                return existing;
            }
            evictIfNecessary();
            DuckDbOperator contextOperator = createContextOperator();

            PerSourceContext context = new PerSourceContext(
                contextKey, contextOperator, sourceId, tableId, schemaInfo);
            context.setBatchSize(batchSize);
            context.setTargetTableName(targetTableName);

            sourceContexts.put(contextKey, context);
            sourceAccessOrder.put(contextKey, Boolean.TRUE);

            logger.debug("Created new Context: {}, sourceId={}, tableId={}, schema={}",
                        contextKey, sourceId, tableId,
                        schemaInfo != null ? "loaded" : "null");

            return context;
        }
    }

    private PerSourceContext getOrCreateContext(String contextKey, String targetTableName) {
        return getOrCreateContext(contextKey, targetTableName, null, null, null);
    }



    private DuckDbOperator createContextOperator() {
        try {
            // 使用与主 Operator 相同的 dbPath 配置
            return new DuckDbOperatorImpl(dbPath, false, batchSize, commitIntervalMs);
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

    // ========== 新增: 确定主表信息并计算默认值 ==========
    private void resolveMainTableInfo() {
        // 1. 确定主表
        if (StringUtils.isBlank(mainTableName)) {
            if (fromTables != null && !fromTables.isEmpty()) {
                FromTableConfig firstFromTable = fromTables.get(0);
                if (firstFromTable == null) {
                    throw new IllegalStateException("First fromTableConfig is null, cannot resolve mainTableName");
                } else {
                    if (StringUtils.isEmpty(firstFromTable.getTableNameInSql())){
                        throw new IllegalStateException("Cannot resolve mainTableName, no schema info for tableNameInSql: " + firstFromTable.getTableNameInSql());
                    } else {
                        mainTableName = firstFromTable.getTableNameInSql();
                        logger.info("Resolved MainTableName: {}", mainTableName);
                    }
                }
            }
        }

        // 2. 确定主表主键（如果未配置，从第一个 fromTable 配置中获取）
        if (StringUtils.isBlank(mainTablePrimaryKey)) {
            NodeSchemaInfo schemaInfo = getNodeSchema(fromTables.get(0).getPreNodeId());
            if (schemaInfo == null) {
                throw new IllegalStateException("Cannot resolve mainTablePrimaryKey，schemaInfo is null，mainTableName: " + mainTableName);
            } else {
                List<String> pkList = schemaInfo.getPrimaryKeys();
                if (pkList != null && !pkList.isEmpty()) {
                    //todo: 组合主键处理：目前先取第一个，后续可以考虑支持复合主键
                    mainTablePrimaryKey = pkList.get(0);
                } else {
                    throw new IllegalStateException("Cannot resolve mainTablePrimaryKey，no primary keys defined in schemaInfo for mainTableName: " + mainTableName);
                }
            }
            if (StringUtils.isBlank(mainTablePrimaryKey)) {
                throw new IllegalStateException("mainTablePrimaryKey is blank after resolution, this should not happen");
            }
            logger.info("Resolved mainTablePrimaryKey: {}", mainTablePrimaryKey);
        }

        // 3. 确定宽表名
        if (StringUtils.isBlank(wideTableName)) {
            if (StringUtils.isNotBlank(mainTableName)) {
                wideTableName = "wide_" + mainTableName;
            } else {
                throw new IllegalStateException("Cannot resolve wideTableName: mainTableName is blank");
            }
            logger.info("Resolved wideTableName: {}", wideTableName);
        }

        // 4. 确定宽表主键
        if (StringUtils.isBlank(wideTablePrimaryKey)) {
            wideTablePrimaryKey = mainTablePrimaryKey;
            logger.info("Resolved wideTablePrimaryKey from mainTablePrimaryKey: {}", wideTablePrimaryKey);
        }

        // 验证关键配置
        if (StringUtils.isBlank(wideTablePrimaryKey)) {
            throw new IllegalStateException("wideTablePrimaryKey is blank after resolution, this should not happen");
        }
    }

    /**
     * 构建带 nodeId 的 dbPath
     * 规则：
     * 1. dbPath 和 nodeId 都不能为空，否则抛出 IllegalArgumentException
     * 2. 去掉末尾的路径分隔符（/ 或 \）
     * 3. 用 _ 连接 dbPath 和 nodeId
     */
    public static String buildDbPathWithNodeId(String dbPath, String nodeId) {
        if (dbPath == null || dbPath.isEmpty()) {
            throw new IllegalArgumentException("dbPath cannot be null or empty");
        }
        if (nodeId == null || nodeId.isEmpty()) {
            throw new IllegalArgumentException("nodeId cannot be null or empty");
        }
        
        // 去掉末尾的路径分隔符（/ 或 \）
        String result = dbPath;
        while (result.endsWith("/") || result.endsWith("\\")) {
            result = result.substring(0, result.length() - 1);
        }
        
        // 用 _ 连接 dbPath 和 nodeId
//        return result + "_" + nodeId;
        return result;
    }
}
