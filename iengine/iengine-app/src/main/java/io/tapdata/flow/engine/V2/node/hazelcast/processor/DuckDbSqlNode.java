package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.processor.context.ProcessContextEvent;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.dag.process.ProcessorNode;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.duckdb.DuckDbOperator;
import io.tapdata.flow.engine.V2.node.duckdb.DuckDbOperatorImpl;
import io.tapdata.flow.engine.V2.node.duckdb.PerSourceContext;
import io.tapdata.flow.engine.V2.node.duckdb.SmartMerger;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.concurrent.ConcurrentHashMap;

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
    private final Map<String, PerSourceContext> sourceContexts = new ConcurrentHashMap<>();

    public DuckDbSqlNode(ProcessorBaseContext processorBaseContext) {
        super(processorBaseContext);
    }

    @Override
    protected void doInit(@NotNull Context context) throws TapCodeException {
        super.doInit(context);
        
        // 初始化 DuckDB 操作器（内存数据库模式）
        try {
            duckDbOperator = new DuckDbOperatorImpl(true, batchSize, 5000);
            logger.info("DuckDbSqlNode initialized with batchSize={}", batchSize);
        } catch (SQLException e) {
            throw new TapCodeException("Failed to initialize DuckDbOperator", e);
        }
    }

    @Override
    protected void tryProcess(TapdataEvent tapdataEvent, BiConsumer<TapdataEvent, ProcessResult> consumer) {
        if (tapdataEvent == null) {
            return;
        }

        TapEvent tapEvent = tapdataEvent.getTapEvent();
        
        // 处理 DML 事件
        if (tapEvent instanceof TapRecordEvent) {
            processRecordEvent((TapRecordEvent) tapEvent, tapdataEvent, consumer);
        } else {
            // 非 DML 事件直接透传
            consumer.accept(tapdataEvent, null);
        }
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
        return sourceContexts.computeIfAbsent(contextKey, key -> {
            PerSourceContext context = new PerSourceContext(key, duckDbOperator);
            context.setBatchSize(batchSize);
            context.setTargetTableName(targetTableName);
            return context;
        });
    }

    private void flushAllContexts() {
        for (PerSourceContext context : sourceContexts.values()) {
            try {
                flushContext(context);
            } catch (Exception e) {
                logger.warn("Failed to flush context {} during close: {}", context.getKey(), e.getMessage(), e);
            }
        }
    }

    @Override
    protected void doClose() {
        super.doClose();
        flushAllContexts();

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
