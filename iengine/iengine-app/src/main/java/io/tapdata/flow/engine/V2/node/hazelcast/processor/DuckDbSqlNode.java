package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.SyncStage;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.processor.context.ProcessContextEvent;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.dag.process.ProcessorNode;
import com.mongodb.client.MongoDatabase;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.duckdb.DuckDbOperator;
import io.tapdata.flow.engine.V2.node.duckdb.DuckDbOperatorImpl;
import io.tapdata.flow.engine.V2.node.duckdb.DlqWriter;
import io.tapdata.flow.engine.V2.node.duckdb.PerSourceContext;
import io.tapdata.flow.engine.V2.node.duckdb.SmartMerger;
import io.tapdata.flow.engine.V2.node.duckdb.ErrorHandler;
import io.tapdata.flow.engine.V2.node.duckdb.MultiTableInputManager;
import io.tapdata.flow.engine.V2.node.duckdb.SyncStageTracker;
import io.tapdata.flow.engine.V2.node.duckdb.OutputBuffer;
import io.tapdata.flow.engine.V2.node.duckdb.DuckDbQueryEngine;
import io.tapdata.flow.engine.V2.node.duckdb.SchemaRegistry;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
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
public class DuckDbSqlNode extends HazelcastProcessorBaseNode {

    private static final Logger logger = LogManager.getLogger(DuckDbSqlNode.class);
    public static final String TAG = DuckDbSqlNode.class.getSimpleName();

    /** DuckDB 操作器 */
    private DuckDbOperator duckDbOperator;

    /** 当前表名 */
    private String currentTableName;

    /** 是否已初始化表结构 */
    private volatile boolean tableInitialized = false;

    /** 批处理缓冲区 */
    private final List<Map<String, Object>> batchBuffer = new ArrayList<>();
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

    private static final int ERROR_THRESHOLD_COUNT = 100;
    private static final double ERROR_THRESHOLD_RATE = 0.01; // 1%
    private static final int OUTPUT_BATCH_SIZE = 1000;
    private static final int QUERY_TIMEOUT_MS = 5000;

    // SQL configuration
    private String querySql = "SELECT * FROM %s";
    private String outputTableName = "duckdb_output";
    private boolean executeQueryOnFullSyncComplete = true;
    private volatile boolean queryExecuted = false;

    // Pending events to emit
    private final Queue<TapdataEvent> pendingEvents = new LinkedList<>();
    private BiConsumer<TapdataEvent, ProcessResult> currentConsumer;

    public DuckDbSqlNode(ProcessorBaseContext processorBaseContext) {
        super(processorBaseContext);
    }

    @Override
    protected void doInit(@NotNull Context context) throws TapCodeException {
        super.doInit(context);
        
        io.tapdata.flow.engine.V2.node.duckdb.DuckLakeConfig duckLakeConfig = 
                io.tapdata.flow.engine.V2.node.duckdb.DuckLakeConfig.disabled();

        // 读取节点配置
        try {
            com.tapdata.tm.commons.dag.process.DuckDbSqlNode nodeConfig =
                    (com.tapdata.tm.commons.dag.process.DuckDbSqlNode) getNode();

            if (nodeConfig != null) {
                // 读取 SQL 查询
                if (nodeConfig.getSqlQuery() != null) {
                    this.querySql = nodeConfig.getSqlQuery();
                } else if (nodeConfig.getQuerySql() != null) {
                    this.querySql = nodeConfig.getQuerySql();
                }

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

                logger.info("DuckDbSqlNode loaded config: querySql={}, outputTableName={}, batchSize={}, executeQueryOnFullSyncComplete={}, duckLake={}",
                        querySql, outputTableName, batchSize, executeQueryOnFullSyncComplete, duckLakeConfig.isEnabled());
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

            // Initialize integration helpers for full/CDC flow
            multiTableInputManager = new MultiTableInputManager();
            syncStageTracker = new SyncStageTracker();
            outputBuffer = new OutputBuffer(OUTPUT_BATCH_SIZE);
            queryEngine = new DuckDbQueryEngine(1024, QUERY_TIMEOUT_MS);
            errorHandler = new ErrorHandler(ERROR_THRESHOLD_COUNT, ERROR_THRESHOLD_RATE);
            schemaRegistry = new SchemaRegistry();

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
     * Handle the transition when all tables have entered CDC stage
     */
    private void handleAllTablesCdcTransition() {
        logger.info("All tables transitioned to CDC, executing query and emitting results...");

        if (executeQueryOnFullSyncComplete && !queryExecuted) {
            try {
                // First, flush all remaining data
                flushAllContexts();

                // Execute query and emit results
                executeAndEmitQueryResults();

                queryExecuted = true;
                logger.info("Query executed and results emitted successfully");
            } catch (Exception e) {
                logger.error("Error executing query on full sync complete: {}", e.getMessage(), e);
            }
        }
    }

    /**
     * Execute SQL query and emit results as TapdataEvents
     */
    private void executeAndEmitQueryResults() {
        try {
            // Determine which tables to query
            List<String> tableNames = new ArrayList<>();
            for (PerSourceContext context : sourceContexts.values()) {
                if (context.getTargetTableName() != null) {
                    tableNames.add(context.getTargetTableName());
                }
            }

            if (tableNames.isEmpty()) {
                logger.warn("No tables to query, skipping result emission");
                return;
            }

            // For each table, execute query
            for (String tableName : tableNames) {
                String sql = String.format(querySql, tableName);
                logger.info("Executing query for table {}: {}", tableName, sql);

                List<Map<String, Object>> results = duckDbOperator.executeQuery(sql);

                if (results != null && !results.isEmpty()) {
                    logger.info("Query returned {} results for table {}", results.size(), tableName);

                    // Emit results as TapInsertRecordEvents
                    for (Map<String, Object> result : results) {
                        emitResultAsTapEvent(result, tableName);
                    }
                } else {
                    logger.info("Query returned no results for table {}", tableName);
                }
            }

        } catch (Exception e) {
            logger.error("Error executing and emitting query results: {}", e.getMessage(), e);
        }
    }

    /**
     * Emit a single result as a TapdataEvent
     */
    private void emitResultAsTapEvent(Map<String, Object> result, String tableName) {
        try {
            // Create TapInsertRecordEvent
            io.tapdata.entity.event.dml.TapInsertRecordEvent insertEvent =
                    new io.tapdata.entity.event.dml.TapInsertRecordEvent();
            insertEvent.setTableId(tableName);
            insertEvent.setAfter(result);

            // Create TapdataEvent
            TapdataEvent tapdataEvent = new TapdataEvent();
            tapdataEvent.setTapEvent(insertEvent);
            tapdataEvent.setSyncStage(com.tapdata.entity.SyncStage.CDC);

            // Add to pending events queue
            pendingEvents.offer(tapdataEvent);

            logger.debug("Added query result to pending events for table: {}", tableName);

        } catch (Exception e) {
            logger.warn("Failed to emit result as TapEvent: {}", e.getMessage());
        }
    }

    public void setQuerySql(String querySql) {
        this.querySql = querySql;
    }

    public void setOutputTableName(String outputTableName) {
        this.outputTableName = outputTableName;
    }

    public void setExecuteQueryOnFullSyncComplete(boolean executeQueryOnFullSyncComplete) {
        this.executeQueryOnFullSyncComplete = executeQueryOnFullSyncComplete;
    }

    @Override
    protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
        // Save the current consumer for emitting pending events
        currentConsumer = consumer;

        // First, emit any pending events
        emitPendingEvents();

        if (tapdataEvent == null) {
            return;
        }

        TapEvent tapEvent = tapdataEvent.getTapEvent();

        // Record event in error handler for rate tracking
        if (errorHandler != null) {
            errorHandler.recordEvent();

            // Check if task should stop due to error threshold
            if (errorHandler.shouldStopTask()) {
                logger.error("Error threshold exceeded. Task should stop.");
                // Still process the event, but subsequent events will also check this
                return;
            }
        }

        // Track sync stage for the event
        trackSyncStage(tapdataEvent);

        // 处理 DML 事件
        if (tapEvent instanceof TapRecordEvent) {
            processRecordEventWithStage((TapRecordEvent) tapEvent, tapdataEvent, consumer);
        } else {
            // 非 DML 事件直接透传
            consumer.accept(tapdataEvent, null);
        }
    }

    /**
     * Emit any pending events
     */
    private void emitPendingEvents() {
        if (currentConsumer != null && !pendingEvents.isEmpty()) {
            TapdataEvent event;
            while ((event = pendingEvents.poll()) != null) {
                try {
                    currentConsumer.accept(event, null);
                } catch (Exception e) {
                    logger.warn("Failed to emit pending event: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * Track sync stage from tapdataEvent
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
     * Process record event with sync stage awareness
     */
    private void processRecordEventWithStage(TapRecordEvent recordEvent, TapdataEvent tapdataEvent,
                                             BiConsumer<TapdataEvent, ProcessResult> consumer) {
        try {
            // 获取表名
            String tableId = TapEventUtil.getTableId(recordEvent);
            String sourceId = resolveSourceId(tapdataEvent, recordEvent);
            if (tableId == null || tableId.isEmpty()) {
                tableId = "unknown_table";
            }
            String contextKey = buildContextKey(sourceId, tableId);
            String targetTableName = buildTargetTableName(sourceId, tableId);
            PerSourceContext context = getOrCreateContext(contextKey, targetTableName);
            currentTableName = targetTableName;

            // 获取记录数据
            Map<String, Object> recordData = extractRecordData(recordEvent);

            if (recordData != null && !recordData.isEmpty()) {
                synchronized (context.getCommitLock()) {
                    context.addRecord(recordData);
                    if (context.getBatchBuffer().size() >= context.getBatchSize()) {
                        flushContext(context);
                    }
                }
            }

            // 检查是否需要执行输出逻辑
            handleOutputWithStage(tapdataEvent, consumer);

        } catch (Exception e) {
            logger.error("Failed to process record event: {}", e.getMessage(), e);

            // Record error in handler
            if (errorHandler != null) {
                try {
                    Map<String, Object> sourceData = extractRecordData(recordEvent);
                    errorHandler.recordError(sourceData != null ? sourceData : new HashMap<>(), e);

                    // Write to DLQ if needed
                    if (dlqWriter != null && sourceData != null) {
                        try {
                            String tableId = TapEventUtil.getTableId(recordEvent);
                            String sourceId = resolveSourceId(tapdataEvent, recordEvent);
                            String contextKey = buildContextKey(sourceId, tableId);
                            dlqWriter.write(contextKey, buildTargetTableName(sourceId, tableId),
                                    Collections.singletonList(sourceData), e);
                        } catch (Exception dlqError) {
                            logger.warn("Failed to write to DLQ: {}", dlqError.getMessage());
                        }
                    }
                } catch (Exception handlerError) {
                    logger.warn("Failed to record error in ErrorHandler: {}", handlerError.getMessage());
                }
            }

            // 处理失败时仍然透传事件
            consumer.accept(tapdataEvent, null);
        }
    }

    /**
     * Handle output logic based on sync stage
     */
    private void handleOutputWithStage(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
        if (syncStageTracker != null) {
            // Check if all tables have transitioned to CDC
            if (syncStageTracker.isTransitionCompleted()) {
                // CDC stage: emit events normally or in batches
                handleCdcOutput(tapdataEvent, consumer);
            } else {
                // Initial sync stage: cache events, don't emit yet
                handleInitialSyncOutput(tapdataEvent, consumer);
            }
        } else {
            // No stage tracking: fall back to normal behavior
            consumer.accept(tapdataEvent, null);
        }
    }

    /**
     * Handle output during initial sync stage
     */
    private void handleInitialSyncOutput(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
        // During initial sync, we don't emit events immediately
        // Events are cached in DuckDB and will be emitted after all tables transition to CDC
        logger.debug("Initial sync in progress, caching event for later emission");

        // Still pass through non-record events
        if (!(tapdataEvent.getTapEvent() instanceof TapRecordEvent)) {
            consumer.accept(tapdataEvent, null);
        }
    }

    /**
     * Handle output during CDC stage
     */
    private void handleCdcOutput(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
        // In CDC stage, we can emit events in batches
        // For now, just pass through normally
        consumer.accept(tapdataEvent, null);
    }

    /**
     * 处理记录事件
     */
    private void processRecordEvent(TapRecordEvent recordEvent, TapdataEvent tapdataEvent,
                                    BiConsumer<TapdataEvent, ProcessResult> consumer) {
        try {
            // 获取表名
            String tableId = TapEventUtil.getTableId(recordEvent);
            String sourceId = resolveSourceId(tapdataEvent, recordEvent);
            if (tableId == null || tableId.isEmpty()) {
                tableId = "unknown_table";
            }
            String contextKey = buildContextKey(sourceId, tableId);
            String targetTableName = buildTargetTableName(sourceId, tableId);
            PerSourceContext context = getOrCreateContext(contextKey, targetTableName);
            currentTableName = targetTableName;

            // 获取记录数据
            Map<String, Object> recordData = extractRecordData(recordEvent);

            if (recordData != null && !recordData.isEmpty()) {
                synchronized (context.getCommitLock()) {
                    context.addRecord(recordData);
                    if (context.getBatchBuffer().size() >= context.getBatchSize()) {
                        flushContext(context);
                    }
                }
            }

            // 将原始事件透传下去
            consumer.accept(tapdataEvent, null);

        } catch (Exception e) {
            logger.error("Failed to process record event: {}", e.getMessage(), e);

            // Record error in handler
            if (errorHandler != null) {
                try {
                    Map<String, Object> sourceData = extractRecordData(recordEvent);
                    errorHandler.recordError(sourceData != null ? sourceData : new HashMap<>(), e);

                    // Write to DLQ if needed
                    if (dlqWriter != null && sourceData != null) {
                        try {
                            String tableId = TapEventUtil.getTableId(recordEvent);
                            String sourceId = resolveSourceId(tapdataEvent, recordEvent);
                            String contextKey = buildContextKey(sourceId, tableId);
                            dlqWriter.write(contextKey, buildTargetTableName(sourceId, tableId),
                                    Collections.singletonList(sourceData), e);
                        } catch (Exception dlqError) {
                            logger.warn("Failed to write to DLQ: {}", dlqError.getMessage());
                        }
                    }
                } catch (Exception handlerError) {
                    logger.warn("Failed to record error in ErrorHandler: {}", handlerError.getMessage());
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
     * 刷新批处理缓冲区
     */
    private void flushBatch() {
        if (batchBuffer.isEmpty()) {
            return;
        }

        List<Map<String, Object>> dataToWrite;
        synchronized (batchBuffer) {
            dataToWrite = new ArrayList<>(batchBuffer);
            batchBuffer.clear();
        }

        if (dataToWrite.isEmpty()) {
            return;
        }

        try {
            // 确保表存在
            ensureTableExists(dataToWrite);

            // 使用 SmartMerger 做简单的 last-wins 合并，减少重复/冗余操作
            List<Map<String, Object>> merged = SmartMerger.mergeLastWins(dataToWrite);

            if (merged == null || merged.isEmpty()) {
                logger.debug("No merged records to write for table: {}", currentTableName);
                return;
            }

            // 优先使用 upsert 批量写入以保证幂等性和 PK 迁移的基本支持
            duckDbOperator.upsertBatch(currentTableName, merged);
            logger.debug("Flushed {} merged records to DuckDB table: {} (original {})", merged.size(), currentTableName, dataToWrite.size());

        } catch (Exception e) {
            logger.error("Failed to flush batch to DuckDB: {}", e.getMessage(), e);
            // 将数据重新放回缓冲区
            synchronized (batchBuffer) {
                batchBuffer.addAll(0, dataToWrite);
            }
        }
    }

    private void flushContext(PerSourceContext context) throws SQLException {
        if (context == null) {
            return;
        }

        List<Map<String, Object>> dataToWrite = context.drainBuffer();
        if (dataToWrite.isEmpty()) {
            return;
        }

        try {
            ensureTableExists(context, dataToWrite);

            List<Map<String, Object>> merged = SmartMerger.mergeLastWins(dataToWrite);
            if (merged == null || merged.isEmpty()) {
                logger.debug("No merged records to write for context: {}", context.getKey());
                return;
            }

            DuckDbOperator operator = context.getOperator() != null ? context.getOperator() : duckDbOperator;
            if (operator == null) {
                throw new SQLException("DuckDbOperator not initialized");
            }

            operator.upsertBatch(context.getTargetTableName(), merged);
            logger.debug("Flushed {} merged records to DuckDB table: {} (original {})", merged.size(), context.getTargetTableName(), dataToWrite.size());
        } catch (Exception e) {
            logger.error("Failed to flush context {} to DuckDB: {}", context.getKey(), e.getMessage(), e);
            synchronized (context.getCommitLock()) {
                context.getBatchBuffer().addAll(0, dataToWrite);
                context.getAccumulatedRecordCount().addAndGet(dataToWrite.size());
            }
            writeToDlq(context, dataToWrite, e);
        }
    }

    /**
     * 确保目标表存在，如果不存在则创建
     */
    private void ensureTableExists(List<Map<String, Object>> data) throws SQLException {
        if (tableInitialized && currentTableName != null) {
            return;
        }

        if (data == null || data.isEmpty()) {
            return;
        }

        // 根据第一条数据推断表结构
        TapTable tapTable = inferTapTable(data, currentTableName);

        if (tapTable != null && tapTable.getNameFieldMap() != null && !tapTable.getNameFieldMap().isEmpty()) {
            // 创建临时表用于处理
            String tempTableName = "temp_" + currentTableName;
            duckDbOperator.createTempTable(tapTable, tempTableName);
            tableInitialized = true;
            logger.info("Created temp table: {}", tempTableName);
        }
    }

    private void ensureTableExists(PerSourceContext context, List<Map<String, Object>> data) throws SQLException {
        if (context.isTableInitialized() && context.getTargetTableName() != null) {
            return;
        }
        if (data == null || data.isEmpty()) {
            return;
        }

        TapTable tapTable = inferTapTable(data, context.getTargetTableName());
        if (tapTable != null && tapTable.getNameFieldMap() != null && !tapTable.getNameFieldMap().isEmpty()) {
            DuckDbOperator operator = context.getOperator() != null ? context.getOperator() : duckDbOperator;
            if (operator == null) {
                throw new SQLException("DuckDbOperator not initialized");
            }
            operator.createTempTable(tapTable, context.getTargetTableName());
            context.setTableInitialized(true);
            logger.info("Created temp table for context {}: {}", context.getKey(), context.getTargetTableName());
        }
    }

    /**
     * 从数据中推断 TapTable 结构
     */
    private TapTable inferTapTable(List<Map<String, Object>> data, String tableName) {
        if (data == null || data.isEmpty()) {
            return null;
        }

        TapTable tapTable = new TapTable(tableName);
        Map<String, Object> firstRow = data.get(0);

        for (Map.Entry<String, Object> entry : firstRow.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();

            io.tapdata.entity.schema.TapField tapField = new io.tapdata.entity.schema.TapField();
            tapField.name(fieldName);
            tapField.tapType(inferTapType(value));
            tapField.dataType(inferDataType(value));

            tapTable.add(tapField);
        }

        return tapTable;
    }

    /**
     * 根据值推断 TapType
     */
    private io.tapdata.entity.schema.type.TapType inferTapType(Object value) {
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

    private io.tapdata.entity.schema.type.TapType createTapString() {
        return new io.tapdata.entity.schema.type.TapString();
    }

    private io.tapdata.entity.schema.type.TapType createTapNumber() {
        return new io.tapdata.entity.schema.type.TapNumber();
    }

    private io.tapdata.entity.schema.type.TapType createTapBoolean() {
        return new io.tapdata.entity.schema.type.TapBoolean();
    }

    private io.tapdata.entity.schema.type.TapType createTapDate() {
        return new io.tapdata.entity.schema.type.TapDate();
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

        // 刷新剩余数据
        if (!batchBuffer.isEmpty()) {
            flushBatch();
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
