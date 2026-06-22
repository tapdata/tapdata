package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataCompleteSnapshotEvent;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.check.DAGCheckUtil;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.DuckDbSqlNode;
import com.tapdata.tm.commons.dag.process.dto.TapTableDto;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.duckdb.AffectedKeyCalculator;
import io.tapdata.flow.engine.V2.node.duckdb.DlqWriter;
import io.tapdata.flow.engine.V2.node.duckdb.DuckDbOperator;
import io.tapdata.flow.engine.V2.node.duckdb.DuckDbOperatorImpl;
import io.tapdata.flow.engine.V2.node.duckdb.DuckDbSqlConfig;
import io.tapdata.flow.engine.V2.node.duckdb.DuckLakeConfig;
import io.tapdata.flow.engine.V2.node.duckdb.ErrorHandler;
import io.tapdata.flow.engine.V2.node.duckdb.FromTableConfig;
import io.tapdata.flow.engine.V2.node.duckdb.NodeSchemaInfo;
import io.tapdata.flow.engine.V2.node.duckdb.PerSourceContext;
import io.tapdata.flow.engine.V2.node.duckdb.QuerySqlProcessor;
import io.tapdata.flow.engine.V2.node.duckdb.SmartMerger;
import io.tapdata.flow.engine.V2.node.duckdb.SyncStageTracker;
import io.tapdata.flow.engine.V2.node.duckdb.WideTableDdlGenerator;
import io.tapdata.flow.engine.V2.node.duckdb.WideTableIncrementalUpdater;
import io.tapdata.flow.engine.V2.node.duckdb.WithCteSqlGenerator;
import io.tapdata.flow.engine.V2.util.NodeUtil;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import io.tapdata.pdk.core.utils.CommonUtils;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * DuckDB SQL 处理器节点
 * 继承 HazelcastProcessorBaseNode，支持流式处理模式
 * 使用 Arrow 零拷贝写入实现高性能数据处理
 */
@Getter
@Setter
public class HazelcastDuckDbSqlNode extends HazelcastProcessorBaseNode {
    protected static final String DEFAULT_DUCK_DB_PATH = "DEFAULT_DUCK_DB_PATH";

    /**
     * DuckDB 操作器
     */
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
     * <p>
     * [nodeSchemaCache]Key: 前置节点的唯一标识 (sourceId/nodeId)
     * [tableSchemaCache]Key: 前置节点的唯一标识 (sourceId/nodeId)
     * Value: 该节点的完整 Schema 信息 (NodeSchemaInfo)
     * <p>
     * 包含信息：
     * - tableName (表名)
     * - qualifiedName (完全限定名，全局唯一)
     * - primaryKeys (主键列表)
     * - fieldMap (字段名 → 字段定义映射)
     * - fieldTypeMap (字段名 → 字段类型映射)
     * <p>
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
    private final int commitIntervalMs = 500;
    private ScheduledExecutorService contextFlusher;
    private DlqWriter dlqWriter;

    // Integration helpers for full/CDC flow
    private SyncStageTracker syncStageTracker;
    private ErrorHandler errorHandler;
    private AffectedKeyCalculator affectedKeyCalculator;
    private WideTableIncrementalUpdater wideTableUpdater;

    private static final int ERROR_THRESHOLD_COUNT = 100;
    private static final double ERROR_THRESHOLD_RATE = 0.01; // 1%
    private static final int OUTPUT_BATCH_SIZE = 1000;
    private static final int QUERY_TIMEOUT_MS = 5000;
    private String querySql = "SELECT * FROM %s";
    private String wideTableName;
    private String dbPath;
    private boolean executeQueryOnFullSyncComplete = false;
    private List<String> wideTablePrimaryKey;
    private boolean outputChangelogEnabled = false;
    private String mainTableName;
    private List<FromTableConfig> fromTables = new ArrayList<>();
    private Map<String, String> customJoinQueries = new HashMap<>();
    private volatile AtomicReference<BiConsumer<TapdataEvent, ProcessResult>> currentConsumer = new AtomicReference<>(null);

    public HazelcastDuckDbSqlNode(ProcessorBaseContext processorBaseContext) {
        super(processorBaseContext);
    }

    static String initDBPath(com.tapdata.tm.commons.dag.process.DuckDbSqlNode nodeConfig) {
        String path = CommonUtils.getProperty(DEFAULT_DUCK_DB_PATH);
        // 读取 dbPath 配置（三级优先级：节点 > 全局 > 默认null）
        if (nodeConfig.getDbPath() != null && !nodeConfig.getDbPath().trim().isEmpty()) {
            path = nodeConfig.getDbPath().trim();
        }
        if (path != null && !path.isEmpty()) {
            File file = new File(path);
            if (!file.exists()) {
                file.mkdirs();
            }
            if (!file.isDirectory()) {
                throw new IllegalArgumentException("Path is not a directory, Cannot be used as a working directory for duck db: " + path);
            }
            String nodeId = nodeConfig.getId();
            if (nodeId != null && !nodeId.isEmpty()) {
                path = buildDbPathWithNodeId(path, nodeConfig.getId());
            }
        }
        return path;
    }

    static DuckDbOperatorImpl initDuckDbOperator(DuckDbSqlNode node, String dbPath, int batchSize) {
        // 读取 DuckLake 配置（支持三级优先级：节点 > 全局 > 默认值）
        DuckLakeConfig duckLakeConfig = DuckLakeConfig.disabled();
        Boolean nodeDuckLakeEnabled = node.getDuckLakeEnabled();
        if (Boolean.TRUE.equals(nodeDuckLakeEnabled) ||
                (nodeDuckLakeEnabled == null && DuckDbSqlConfig.isDuckLakeEnabled())) {
            // 使用节点配置或回退到全局配置
            String storageType = node.getDuckLakeStorageType() != null ?
                    node.getDuckLakeStorageType() : DuckDbSqlConfig.getDuckLakeStorageType();
            String storagePath = node.getDuckLakeStoragePath() != null ?
                    node.getDuckLakeStoragePath() : DuckDbSqlConfig.getDuckLakeStoragePath();
            String metadataUrl = node.getDuckLakeMetadataDbUrl() != null ?
                    node.getDuckLakeMetadataDbUrl() : DuckDbSqlConfig.getDuckLakeMetadataDbUrl();
            duckLakeConfig = new io.tapdata.flow.engine.V2.node.duckdb.DuckLakeConfig(
                    true, storageType, storagePath, metadataUrl
            );
        }
        try {
            return new DuckDbOperatorImpl(dbPath, false, batchSize, 5000, duckLakeConfig);
        } catch (Exception e) {
            throw new TapCodeException("Failed to initialize DuckDbOperator", e);
        }
    }

    protected void initIndex(DuckDbSqlNode node) {
        this.wideTablePrimaryKey = new ArrayList<>();
        TapTable tapTable = processorBaseContext.getTapTableMap().get(node.getId());
        List<TapIndex> indexList = tapTable.getIndexList();
        if (indexList == null || indexList.isEmpty()) {
            return;
        }
        TapIndex tapIndex = indexList.get(0);
        tapIndex.getIndexFields()
                .stream()
                .map(TapIndexField::getName)
                .forEach(wideTablePrimaryKey::add);
        Node<?> targetNode = DAGCheckUtil.getTargetNode(node);
        if (!(targetNode instanceof TableNode targetTableNode)) {
            return;
        }
        List<String> updateConditionFields = targetTableNode.getUpdateConditionFields();
        if (updateConditionFields == null || updateConditionFields.isEmpty()) {
            return;
        }
        wideTablePrimaryKey = new ArrayList<>(updateConditionFields);
        obsLogger.info("Merge wide tables using '{}' as the joint index", TapSimplify.toJson(wideTablePrimaryKey));
    }

    @Override
    protected void doInit(@NotNull Context context) throws TapCodeException {
        super.doInit(context);
        if (null == clientMongoOperator) {
            clientMongoOperator = initClientMongoOperator();
        }
        com.tapdata.tm.commons.dag.process.DuckDbSqlNode nodeConfig =
                (com.tapdata.tm.commons.dag.process.DuckDbSqlNode) getNode();
        this.dbPath = initDBPath(nodeConfig);
        obsLogger.info("Database path: {}", dbPath);
        this.duckDbOperator = initDuckDbOperator(nodeConfig, dbPath, OUTPUT_BATCH_SIZE);
        // 读取节点配置
        try {
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
            // ========== 新增: 读取实时增量物化视图配置 ==========
            initIndex(nodeConfig);
            if (nodeConfig.getOutputChangelogEnabled() != null) {
                this.outputChangelogEnabled = nodeConfig.getOutputChangelogEnabled();
            }
            if (nodeConfig.getMainTableName() != null) {
                this.mainTableName = nodeConfig.getMainTableName();
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
                                obsLogger.warn("Failed to convert FromTableConfig: {}", e.getMessage());
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
                obsLogger.info("QuerySql normalized and validated successfully");
            }
            // ========== 新增: 确定主表信息和默认值 ==========
            resolveMainTableInfo();
            obsLogger.info("DuckDbSqlNode loaded config: querySql={}, wideTableName={}, batchSize={}, executeQueryOnFullSyncComplete={}, materializedView={}",
                    querySql, wideTableName, batchSize, executeQueryOnFullSyncComplete,
                    wideTablePrimaryKey != null ? "enabled" : "disabled");
        } catch (Exception e) {
            obsLogger.error("Failed to load DuckDbSqlNode config, using defaults: {}", e.getMessage());
            throw e;
        }
        // 初始化 DuckDB 操作器（支持内存/文件持久化模式）
        try {
            dlqWriter = new DlqWriter(clientMongoOperator, DLQ_COLLECTION);

            // 初始化全量/CDC集成辅助组件
            syncStageTracker = new SyncStageTracker();
            syncStageTracker.setLogger(obsLogger);
            List<String> allTableName = NodeUtil.findAllTableName(nodeConfig);
            allTableName.forEach(name -> syncStageTracker.updateTableStageFromEvent(name, SyncStage.INITIAL_SYNC));
            errorHandler = new ErrorHandler(ERROR_THRESHOLD_COUNT, ERROR_THRESHOLD_RATE);

            // ========== 新增: 初始化实时增量物化视图组件 ==========
            if (wideTablePrimaryKey != null && !wideTablePrimaryKey.isEmpty()) {
                affectedKeyCalculator = new AffectedKeyCalculator(
                        wideTablePrimaryKey,
                        mainTableName,
                        fromTables,
                        customJoinQueries,
                        duckDbOperator,
                        tableSchemaCache,
                        this.querySql
                );

                obsLogger.info("AffectedKeyCalculator initialized with {} schema(s), querySql length={}",
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
                    obsLogger.info("Found wide table NodeSchemaInfo: {}", wideTableSchemaInfo != null ? wideTableSchemaInfo.getTableName() : "not found");
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
                ).log(obsLogger).withDeleteSemantics(io.tapdata.flow.engine.V2.node.duckdb.WideTableSourceRegistry.from(mainTableName, fromTables, nodeSchemaCache));

                obsLogger.info("Materialized view components initialized: affectedKeyCalculator={}, wideTableUpdater={}",
                        affectedKeyCalculator != null, wideTableUpdater != null);
            }

            // Register callback for when all tables enter CDC
            syncStageTracker.setOnAllTablesCdcCallback(isCdc -> {
                try {
                    handleAllTablesCdcTransition(isCdc);
                } catch (Exception e) {
                    obsLogger.error("Error handling all tables CDC transition: {}", e.getMessage(), e);
                }
            });

            // ========== 核心功能：DuckDB 表生命周期管理： ==========
            manageDuckDbTables();

            obsLogger.info("DuckDbSqlNode initialized with batchSize={}, output batch size={}, error threshold={}% (count: {}), schemaCache={}",
                    batchSize, OUTPUT_BATCH_SIZE, ERROR_THRESHOLD_RATE * 100, ERROR_THRESHOLD_COUNT,
                    !nodeSchemaCache.isEmpty() ? "loaded (" + nodeSchemaCache.size() + " nodes)" : "empty");
        } catch (SQLException e) {
            throw new TapCodeException("Failed to initialize DuckDbOperator", e);
        }
    }

    /**
     * 处理所有表进入CDC阶段的转换
     * 当所有表都从全量同步切换到增量同步时触发
     */
    private void handleAllTablesCdcTransition(boolean isCdc) {
        obsLogger.info("All tables have been switched to the CDC stage, executing queries and submitting results");

        try {
            // 步骤1: 刷写所有剩余数据
            flushAllContexts(isCdc);
            // 步骤2: 更新宽表（可选）一次，直接用 insert + querySQL 语句一次性写入
            if (wideTableUpdater != null) {
                try {
                    duckDbOperator.executeInTransaction(this::updateWideTableInFullSyncComplete);
                    obsLogger.info("After the full amount is completed, the wide table update is finished");
                } catch (Exception e) {
                    obsLogger.error("Failed to update the wide table after full completion: {}", e.getMessage(), e);
                }
            }
            // 步骤3: 生成宽表 CDC 事件，直接用 select 查询宽表语句查询出所有结果集拼接 insertEvent
            if (outputChangelogEnabled) {
                try {
                    int emittedCount = generateWideTableInsertEvents();
                    if (emittedCount > 0) {
                        obsLogger.info("Submit {} wide table CDC events to downstream", emittedCount);
                    }
                } catch (Exception e) {
                    obsLogger.error("Failed to generate wide table CDC event: {}", e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            obsLogger.error("Processing failed after completing full synchronization: {}", e.getMessage(), e);
        }
    }

    /**
     * 全量结束后更新宽表，直接用 insert + querySQL 语句一次性写入
     */
    private void updateWideTableInFullSyncComplete() throws SQLException {
        if (querySql == null || querySql.isEmpty()) {
            obsLogger.warn("QuerySQL is empty, skip wide table update");
            return;
        }
        StringJoiner joiner = new StringJoiner(",");
        TapTable tapTable = processorBaseContext.getTapTableMap().get(getNode().getId());
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        nameFieldMap.forEach((name, field) -> {
            joiner.add(field.getName());
        });
        String insertSql = String.format("INSERT INTO %s (%s) %s", wideTableName, joiner, querySql);
        obsLogger.info("Perform wide table update after full completion SQL: {}", insertSql);
        duckDbOperator.executeUpdate(insertSql);
    }

    /**
     * 生成宽表 CDC 事件，直接用 select 查询宽表语句查询出所有结果集拼接 insertEvent
     */
    private int generateWideTableInsertEvents() throws SQLException {
        String selectSql = String.format("SELECT * FROM %s", wideTableName);
        NodeSchemaInfo wideTableSchemaInfo = tableSchemaCache.get(wideTableName);
        int effectiveBatchSize = Math.max(1, batchSize);
        final int[] emittedCount = {0};

        duckDbOperator.executeQueryInBatches(selectSql, effectiveBatchSize, rows -> {
            List<Map<String, Object>> normalizedRows =
                    io.tapdata.flow.engine.V2.node.duckdb.WideTableRowTypeNormalizer.normalizeRows(rows, wideTableSchemaInfo);

            for (Map<String, Object> row : normalizedRows) {
                long now = System.currentTimeMillis();
                TapInsertRecordEvent insertEvent = TapInsertRecordEvent.create()
                        .referenceTime(now)
                        .table(wideTableName)
                        .after(row);

                TapdataEvent tapdataEvent = new TapdataEvent();
                tapdataEvent.setSourceTime(now);
                tapdataEvent.setTapEvent(insertEvent);
                tapdataEvent.setSyncStage(Optional.ofNullable(syncStage).orElse(SyncStage.INITIAL_SYNC));

                currentConsumer.get().accept(tapdataEvent, getProcessResult(TapEventUtil.getTableId(tapdataEvent.getTapEvent())));
                emittedCount[0]++;
            }
        });

        if (emittedCount[0] == 0) {
            obsLogger.info("There is no data in the wide table, skip generating CDC events");
            return 0;
        }

        obsLogger.info("Join the table and obtain a wide table record with {} rows", emittedCount[0]);
        return emittedCount[0];
    }

    @Override
    protected void processIgnoreEvent(TapdataEvent tapdataEvent) {
        if (tapdataEvent instanceof TapdataCompleteSnapshotEvent) {
            syncStageTracker.plusCDC();
        }
    }

    @Override
    protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
        // 步骤1: 保存当前消费者用于发送待处理事件
        currentConsumer.set(consumer);

        // 步骤2: 发送待处理事件:先清旧队列，保证时序：先发上一轮积压事件，再处理当前输入，避免“旧结果被新事件插队”。
        //emitPendingEvents();

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
                obsLogger.error("Exceeding the error threshold, the task should be stopped");
                return;
            }
        }
        if (tapdataEvent instanceof TapdataCompleteSnapshotEvent) {
            syncStageTracker.plusCDC();
        }

        // 步骤6: 事件分类处理
        if (tapEvent instanceof TapRecordEvent e) {
            syncStage = tapdataEvent.getSyncStage();
            // DML事件: 统一处理
            processRecordEvent(e, tapdataEvent);
        } else {
            // 非DML事件: 直接透传
            consumer.accept(tapdataEvent, null);
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
     * @param recordEvent  记录事件
     * @param tapdataEvent 数据事件
     */
    private void processRecordEvent(TapRecordEvent recordEvent, TapdataEvent tapdataEvent) {
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

                obsLogger.debug("Using preloaded schema - sourceId: {}, table: {}, qn: {}, targetTable: {}",
                        sourceId, schemaInfo.getTableName(), schemaInfo.getQualifiedName(),
                        targetTableName);
            } else {
                targetTableName = buildTargetTableName(sourceId, tableId);
                contextKey = buildContextKey(sourceId, tableId);

                obsLogger.debug("Using fallback mode - sourceId: {}, tableId: {}, contextKey: {}",
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

            if (obsLogger.isDebugEnabled() && context.hasSchema()) {
                NodeSchemaInfo ctxSchema = context.getSchema();

                Map<String, Object> after = TapEventUtil.getAfter(recordEvent);
                if (after != null) {
                    for (String fieldName : after.keySet()) {
                        if (ctxSchema.isPrimaryKey(fieldName)) {
                            obsLogger.debug("Primary key field detected: {} (from table {})",
                                    fieldName, ctxSchema.getTableName());
                        }
                    }

                    for (Map.Entry<String, Object> entry : after.entrySet()) {
                        io.tapdata.entity.schema.type.TapType fieldType =
                                ctxSchema.getFieldType(entry.getKey());
                        if (fieldType != null) {
                            obsLogger.debug("Field type: {} -> {}",
                                    entry.getKey(), fieldType.getClass().getSimpleName());
                        }
                    }
                }
            }

            synchronized (context.getCommitLock()) {
                context.addEvent(tapdataEvent);
                if (context.needAccept()) {
                    boolean isInitialSync = syncStageTracker != null && syncStageTracker.isTableInInitialSync(context.getTargetTableName());
                    flushContext(context, !isInitialSync);
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
        obsLogger.error("Failed to process record event: {}", errorContext, e);

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
                        obsLogger.warn("Failed to write DLQ: {}", dlqError.getMessage());
                    }
                }
            } catch (Exception handlerError) {
                obsLogger.warn("Failed to record error info: {}", handlerError.getMessage());
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

            obsLogger.debug("Writing to DLQ with preloaded schema - sourceId: {}, table: {}, qn: {}, targetTable: {}",
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

            obsLogger.debug("Writing to DLQ with fallback mode - sourceId: {}, tableId: {}, contextKey: {}",
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
    private void flushContext(PerSourceContext context, boolean isCdc) throws SQLException {
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
                if (!isCdc) {
                    // ========== 全量阶段 ==========
                    processInitialSyncStage(context, eventsToFlush);
                    context.resetBuffer();
                } else {
                    // ========== 增量阶段 ==========
                    processCdcStage(context, eventsToFlush);
                    //flushAllContextsSafely();
                }

            } catch (Exception e) {
                obsLogger.error("刷写Context {} 到DuckDB失败: {}", context.getKey(), e.getMessage(), e);
                context.getAccumulatedRecordCount().addAndGet(eventsToFlush.size());
                writeToDlq(context, extractDataFromEvents(eventsToFlush), e);
                eventsToFlush.clear();
            }
        }
    }

    /**
     * 处理全量阶段
     * 职责：只负责数据合并和写入 DuckDB，不包含宽表更新逻辑
     * <p>
     * 流程：
     * 1. 使用 SmartMerger 合并事件（第二步、第三步）
     * 2. ensureTableExists()
     * 3. DuckDB 事务开启（可选）
     * 4. Before 的所有数据全部执行 delete 操作
     * 5. After 的数据执行 Arrow 批量写入
     * <p>
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

        obsLogger.debug("全量阶段直接插入 {} 条事件到DuckDB表: {}", eventsToFlush.size(), context.getTargetTableName());
    }

    /**
     * 获取 Context 对应的 DuckDbOperator
     *
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
     * <p>
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
        // 获取 DuckDbOperator
        DuckDbOperator operator = getOperatorForContext(context);
        // 步骤3: 计算 beforeKeys（数据写入 DuckDB 之前）
        List<Map<String, Object>> beforeKeys;
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

        obsLogger.debug("增量阶段刷写 {} 条记录到DuckDB表: {} (原始 {} 条)",
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
     * @param beforeKeys      写入前的主键集合
     * @param afterKeysResult 写入后的主键计算结果（包含PKs、查询结果、afterRows）
     * @param mergedRecords   合并后的记录列表（备用，优先使用 afterKeysResult 中的数据）
     */
    private void updateWideTable(String targetTableName,
                                 List<Map<String, Object>> beforeKeys,
                                 AffectedKeyCalculator.AffectedKeysResult afterKeysResult,
                                 List<SmartMerger.MergedRecord> mergedRecords) {
        if (wideTableUpdater == null || beforeKeys == null || afterKeysResult == null) {
            return;
        }

        try {
            // 使用 AffectedKeysResult 中的预计算数据，避免重复查询
            List<Map<String, Object>> afterKeys = afterKeysResult.getWideTablePks();
            List<Map<String, Object>> afterRows = afterKeysResult.getAfterRows();
            List<Map<String, Object>> wideTableQueryResults = afterKeysResult.getWideTableQueryResults();

            // 传递预计算的查询结果，避免重复执行 WITH CTE 查询
            List<TapdataEvent> wideTableEvents = wideTableUpdater.updateWideTableAsTapDataEvents(
                    beforeKeys, wideTableQueryResults, afterRows, targetTableName, currentConsumer);
        } catch (Exception e) {
            obsLogger.error("更新宽表失败: {}", e.getMessage(), e);
        }
    }

    /**
     * 删除 Before 数据（执行 DELETE 操作）
     *
     * <p>Refactored per 2026-06-07 design: now reads PKs directly from
     * {@link SmartMerger.MergedRecord#getBeforeRows()}, avoiding re-traversal
     * of original operations.</p>
     *
     * @param operator      DuckDB 操作器
     * @param tableName     目标表名
     * @param mergedRecords 合并后的记录列表
     * @param schema        表的 schema 信息（用于获取真实主键）
     * @throws SQLException 如果数据库操作失败
     */
    private void deleteBeforeData(DuckDbOperator operator, String tableName,
                                  List<SmartMerger.MergedRecord> mergedRecords,
                                  NodeSchemaInfo schema) throws SQLException {
        List<Map<String, Object>> list = mergedRecords.stream()
                .map(SmartMerger.MergedRecord::getMainTableBeforePks)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .toList();
        List<Object> beforePks = new ArrayList<>(list);
        if (!beforePks.isEmpty()) {
            obsLogger.debug("Deleting {} before rows from table '{}'", beforePks.size(), tableName);
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
     * @param operator      DuckDB 操作器
     * @param schemaInfo    预加载的Schema信息
     * @param mergedRecords 合并后的记录列表
     * @throws SQLException        如果数据库操作失败
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
                obsLogger.info("Initializing schema cache from nodeConfig, total {} TapTableDtos",
                        nodeConfig.getPreNodeTapTables().size());

                // 使用 IengineSchemaConverter 进行转换
                io.tapdata.flow.engine.V2.node.duckdb.converter.IengineSchemaConverter converter =
                        io.tapdata.flow.engine.V2.node.duckdb.converter.IengineSchemaConverter.getInstance();
                Map<String, NodeSchemaInfo> schemaInfoMap = converter.convert(nodeConfig.getPreNodeTapTables());

                Optional.ofNullable(schemaInfoMap.get(wideTableName)).ifPresent(info -> info.initPrimaryKeys(this.wideTablePrimaryKey));
                // 填充缓存
                int successCount = 0;
                for (Map.Entry<String, NodeSchemaInfo> entry : schemaInfoMap.entrySet()) {
                    NodeSchemaInfo schemaInfo = entry.getValue();
                    if (schemaInfo.isValid()) {
                        nodeSchemaCache.put(entry.getKey(), schemaInfo);
                        tableSchemaCache.put(schemaInfo.getTableName(), schemaInfo);
                        successCount++;
                        obsLogger.debug("Schema cached from nodeConfig: {} -> {}", entry.getKey(), schemaInfo.getQualifiedName());
                    }
                }

                schemaCacheInitialized = true;
                obsLogger.info("Schema cache initialized from nodeConfig: {}/{}",
                        successCount, nodeConfig.getPreNodeTapTables().size());
                return;
            }

            throw new IllegalStateException("No schemas found for schema cache initialization: preNodeTapTables is empty");

        } catch (Exception e) {
            obsLogger.error("Failed to initialize node schema cache: {}", e.getMessage(), e);
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
            obsLogger.warn("duckDbOperator is null, skip table management");
            return;
        }

        // 获取任务重置标识（默认为 false，不重建）
        boolean shouldRecreate = shouldRecreateTables();

        obsLogger.info("Starting DuckDB table management. shouldRecreate={}, fromTables count={}",
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

                    obsLogger.info("Main table managed: {} -> {} (recreate={})",
                            mainTableName, targetTableName, shouldRecreate);
                } catch (Exception e) {
                    throw new TapCodeException("Failed to manage main table: " + mainTableName, e);
                }
            } else {
                obsLogger.warn("Cannot find schema info for main table: {}", mainTableName);
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
                    obsLogger.warn("Cannot find schema info for preNodeId={}, skipping table creation", preNodeId);
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

                    obsLogger.debug("From table managed: {} -> {} (recreate={})",
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

        obsLogger.info("DuckDB table management completed. Created={}, Recreated={}, Total={}",
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
            obsLogger.info("Wide table '{}' already exists, skipping creation", wideTableName);
            return false;
        }

        // 如果需要重建，先删除旧表
        if (tableExists && shouldRecreate) {
            obsLogger.info("Dropping existing wide table '{}' for recreation", wideTableName);
            duckDbOperator.executeUpdate("DROP TABLE IF EXISTS " + WideTableDdlGenerator.quoteIdentifier(wideTableName));
        }

        // 2. 创建宽表
        obsLogger.info("Creating wide table '{}'...", wideTableName);

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
            obsLogger.info("Creating wide table using NodeSchemaInfo: {}", wideTableSchemaInfo.getTableName());
            String createDdl = WideTableDdlGenerator.generateCreateTableDdl(wideTableSchemaInfo);
            duckDbOperator.executeUpdate(createDdl);
            obsLogger.info("Successfully created wide table '{}' from NodeSchemaInfo", wideTableName);
            String createIndexDdl = WideTableDdlGenerator.generateIndex(wideTableSchemaInfo, wideTablePrimaryKey);
            duckDbOperator.executeUpdate(createIndexDdl);
            obsLogger.info("Successfully created wide table index for '{}'", wideTableName);
            created = true;
        } else {
            // 策略2：尝试 CREATE TABLE ... AS SELECT ... WHERE 1=0
            obsLogger.info("Creating wide table using CREATE TABLE AS SELECT...");
            created = createTableUsingAsSelect(wideTableName, querySql);

            // 策略3：降级到传统字段列表方式
            if (!created) {
                obsLogger.warn("CREATE TABLE AS SELECT failed, falling back to traditional DDL");
                createTableUsingFieldList(wideTableName, wideTablePrimaryKey);
                created = true;
            }
        }

        if (created) {
            obsLogger.info("Successfully created wide table '{}'", wideTableName);
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
            obsLogger.warn("Failed to check if table '{}' exists: {}", tableName, e.getMessage());
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
            obsLogger.info("Successfully created table '{}' using CREATE TABLE AS SELECT", tableName);
            return true;
        } catch (Exception e) {
            obsLogger.warn("CREATE TABLE AS SELECT failed for table '{}': {}", tableName, e.getMessage());
            return false;
        }
    }

    /**
     * 使用传统字段列表方式创建表（降级方案）
     */
    private void createTableUsingFieldList(String tableName, List<String> primaryKey) throws SQLException {
        List<String> selectFields = WideTableDdlGenerator.extractSelectFields(querySql);

        if (selectFields.isEmpty()) {
            throw new SQLException("Cannot extract fields from querySql");
        }

        String createDdl = WideTableDdlGenerator.generateCreateTableDdl(tableName, selectFields, primaryKey);
        duckDbOperator.executeUpdate(createDdl);

        obsLogger.info("Successfully created table '{}' using traditional DDL with {} fields",
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
            obsLogger.debug("No fromTables configured, skip SQL alias resolution");
            return;
        }

        if (querySql == null || querySql.isBlank()) {
            obsLogger.warn("querySql is blank, cannot perform alias resolution");
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
                obsLogger.warn("FromTableConfig has blank preNodeId, skipping");
                continue;
            }

            if (tableNameInSql == null || tableNameInSql.isBlank()) {
                obsLogger.warn("FromTableConfig has blank tableNameInSql for preNodeId={}, skipping", preNodeId);
                continue;
            }

            // 通过 preNodeId 查找 NodeSchemaInfo（支持 sourceId 和 nodeId 两种查找方式）
            NodeSchemaInfo schemaInfo = getNodeSchema(preNodeId);

            if (schemaInfo == null) {
                obsLogger.error("Cannot find NodeSchemaInfo for preNodeId={}, " +
                                "SQL alias resolution may be incomplete. " +
                                "Available nodes: {}",
                        preNodeId, nodeSchemaCache.keySet());
                throw new TapCodeException("Failed to find schema info for predecessor node: " + preNodeId);
            }

            // 获取目标表名（格式：sourceId__tableName）
            String targetTableName = schemaInfo.getTargetTableName();

            obsLogger.info("Resolved table alias: '{}' -> '{}' (preNodeId={})",
                    tableNameInSql, targetTableName, preNodeId);

            aliasToTargetMap.put(tableNameInSql, targetTableName);
        }

        if (aliasToTargetMap.isEmpty()) {
            obsLogger.debug("No valid aliases to resolve in querySql");
            return;
        }

        // 使用正则边界检测进行替换
        this.querySql = replaceWithBoundaryDetection(originalSql, aliasToTargetMap);

        obsLogger.info("SQL alias resolution completed. Original: {} -> Resolved: {}", originalSql, this.querySql);
    }

    /**
     * 使用正则边界检测替换 SQL 中的表别名
     *
     * @param sql      原始 SQL 语句
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

            obsLogger.debug("Created new Context: {}, sourceId={}, tableId={}, schema={}",
                    contextKey, sourceId, tableId,
                    schemaInfo != null ? "loaded" : "null");

            return context;
        }
    }


    private DuckDbOperator createContextOperator() {
        try {
            // 使用与主 Operator 相同的 dbPath 配置
            return new DuckDbOperatorImpl(dbPath, false, batchSize, commitIntervalMs);
        } catch (SQLException e) {
            obsLogger.warn("Failed to create dedicated DuckDB operator, fallback to shared operator: {}", e.getMessage());
            return duckDbOperator;
        }
    }

    private void flushAllContexts(boolean isCdc) {
        for (PerSourceContext context : new ArrayList<>(sourceContexts.values())) {
            synchronized (context.getCommitLock()) {
                try {
                    flushContext(context, isCdc);
                } catch (Exception e) {
                    obsLogger.warn("Failed to flush context {} during close: {}", context.getKey(), e.getMessage(), e);
                }
            }
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
                    boolean isInitialSync = syncStageTracker != null && syncStageTracker.isTableInInitialSync(evicted.getTargetTableName());
                    flushContext(evicted, isInitialSync);
                } catch (Exception e) {
                    obsLogger.warn("Failed to flush evicted context {}: {}", eldestKey, e.getMessage(), e);
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
            obsLogger.warn("Failed to close context operator for {}: {}", context.getKey(), e.getMessage(), e);
        }
    }

    private void writeToDlq(PerSourceContext context, List<Map<String, Object>> payload, Exception error) {
        if (dlqWriter == null || context == null) {
            return;
        }
        try {
            dlqWriter.write(context.getKey(), context.getTargetTableName(), payload, error);
        } catch (RuntimeException dlqError) {
            obsLogger.warn("Failed to persist DuckDB DLQ record for {}: {}", context.getKey(), dlqError.getMessage(), dlqError);
        }
    }

    @Override
    protected void doClose() {
        super.doClose();
        if (contextFlusher != null) {
            contextFlusher.shutdownNow();
        }
        flushAllContexts(syncStage == SyncStage.CDC);
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
                obsLogger.info("DuckDbSqlNode closed successfully");
            } catch (Exception e) {
                obsLogger.warn("Failed to close DuckDbOperator: {}", e.getMessage());
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


    // ========== 新增: 确定主表信息并计算默认值 ==========
    private void resolveMainTableInfo() {
        // 1. 确定主表
        if (StringUtils.isBlank(mainTableName)) {
            if (fromTables != null && !fromTables.isEmpty()) {
                FromTableConfig firstFromTable = fromTables.get(0);
                if (firstFromTable == null) {
                    throw new IllegalStateException("First fromTableConfig is null, cannot resolve mainTableName");
                } else {
                    if (StringUtils.isEmpty(firstFromTable.getTableNameInSql())) {
                        throw new IllegalStateException("Cannot resolve mainTableName, no schema info for tableNameInSql: " + firstFromTable.getTableNameInSql());
                    } else {
                        mainTableName = firstFromTable.getTableNameInSql();
                        obsLogger.info("Resolved MainTableName: {}", mainTableName);
                    }
                }
            }
        }
        // 3. 确定宽表名
        if (StringUtils.isBlank(wideTableName)) {
            if (StringUtils.isNotBlank(mainTableName)) {
                wideTableName = "wide_" + mainTableName;
            } else {
                throw new IllegalStateException("Cannot resolve wideTableName: mainTableName is blank");
            }
            obsLogger.info("Resolved wideTableName: {}", wideTableName);
        }

        // 验证关键配置
        if (wideTablePrimaryKey.isEmpty()) {
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
        String result = dbPath;
        while (result.endsWith("/") || result.endsWith("\\")) {
            result = result.substring(0, result.length() - 1);
        }
        return Path.of(dbPath, nodeId).toAbsolutePath().toString();
    }

    @Override
    public boolean needCopyBatchEventWrapper() {
        return true;
    }

    public static void cleanCache(DuckDbSqlNode node) {
        String dbPath = initDBPath(node);
        DuckDbOperator duckDbOperator;
        Set<String> tables = Optional.ofNullable(node.getPreNodeTapTables())
                .orElse(new ArrayList<>())
                .stream()
                .map(TapTableDto::getName)
                .collect(Collectors.toSet());
        try {
            duckDbOperator = initDuckDbOperator(node, dbPath, 100);
        } catch (Exception e) {
            throw new TapCodeException("Failed to initialize DuckDbOperator", e);
        }
        String wideTableName = node.getWideTableName();
        tables.add(wideTableName);
        try {
            dropTable(duckDbOperator, tables);
        } finally {
            try {
                duckDbOperator.close();
            } catch (Exception e) {
                throw new TapCodeException("Failed to close DuckDbOperator", e);
            }
        }
    }

    static void dropTable(DuckDbOperator duckDbOperator, Set<String> tableName) {
        StringJoiner joiner = new StringJoiner(" | ");
        for (String table : tableName) {
            try {
                duckDbOperator.dropTable(table);
            } catch (Exception e) {
                joiner.add(table + " dropped failed: " + e.getMessage());
            }
        }
        if (joiner.length() > 0) {
            throw new TapCodeException(joiner.toString());
        }
    }
}
