package io.tapdata.flow.engine.V2.node.hazelcast.processor;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.processor.context.ProcessContextEvent;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.dag.process.ProcessorNode;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.exception.TapCodeException;
import io.tapdata.flow.engine.V2.node.duckdb.DuckDbOperator;
import io.tapdata.flow.engine.V2.node.duckdb.DuckDbOperatorImpl;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

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
            if (tableId != null && !tableId.isEmpty()) {
                currentTableName = tableId;
            }

            // 获取记录数据
            Map<String, Object> recordData = extractRecordData(recordEvent);
            
            if (recordData != null && !recordData.isEmpty()) {
                // 添加到批处理缓冲区
                synchronized (batchBuffer) {
                    batchBuffer.add(recordData);
                    
                    // 达到批处理大小阈值时刷新
                    if (batchBuffer.size() >= batchSize) {
                        flushBatch();
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
            
            // 使用 DuckDB 操作器写入数据
            duckDbOperator.writeBatch(dataToWrite, currentTableName);
            logger.debug("Flushed {} records to DuckDB table: {}", dataToWrite.size(), currentTableName);
            
        } catch (Exception e) {
            logger.error("Failed to flush batch to DuckDB: {}", e.getMessage(), e);
            // 将数据重新放回缓冲区
            synchronized (batchBuffer) {
                batchBuffer.addAll(0, dataToWrite);
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

    @Override
    protected void doClose() {
        super.doClose();
        
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
