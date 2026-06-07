package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import com.tapdata.tm.commons.dag.process.dto.TapFieldDto;
import com.tapdata.tm.commons.dag.process.dto.TapTableDto;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Arrow零拷贝写入模块
 * 
 * 【决策记录】独立设计，支持真正的 Arrow 零拷贝，必须保证高性能、稳定、调试通过
 * 
 * 设计原则：
 * 1. 模块化隔离：与 DuckDbOperator 解耦，便于独立演进和测试
 * 2. 零拷贝优先：核心目标是实现真正的 Arrow 零拷贝写入，避免数据复制
 * 3. 降级策略：零拷贝不可用时自动降级到 COPY 模式（而非 JDBC 批处理）
 * 4. 性能保证：必须通过性能测试验证，确保写入吞吐量达到预期
 * 5. 稳定性要求：完善的异常处理和资源管理，确保生产环境稳定运行
 * 6. 可调试性：提供详细的日志输出，便于问题定位和性能分析
 * 
 * 核心功能：
 * 1. TapTable → Arrow Schema 转换
 * 2. Map数据 → VectorSchemaRoot 构建
 * 3. Arrow Stream 零拷贝注册到 DuckDB
 * 4. COPY 模式降级写入
 */
public class ArrowWriter implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ArrowWriter.class);

    private final Connection connection;
    private final boolean zeroCopyEnabled;
    private final DuckLakeConfig duckLakeConfig;
    private final BufferAllocator allocator;
    
    // 用于生成唯一的临时表名
    private static volatile int tempTableCounter = 0;

    public ArrowWriter(Connection connection) {
        this(connection, true, DuckLakeConfig.disabled());
    }

    public ArrowWriter(Connection connection, boolean zeroCopyEnabled) {
        this(connection, zeroCopyEnabled, DuckLakeConfig.disabled());
    }

    public ArrowWriter(Connection connection, boolean zeroCopyEnabled, DuckLakeConfig duckLakeConfig) {
        this.connection = connection;
        this.zeroCopyEnabled = zeroCopyEnabled;
        this.duckLakeConfig = duckLakeConfig;
        // 创建 RootAllocator，管理 Arrow 内存
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        logger.info("ArrowWriter initialized with zero-copy: {}, DuckLake: {}", 
            zeroCopyEnabled, duckLakeConfig.isEnabled());
    }

    /**
     * 将TapTable转换为Arrow Schema
     */
    public Schema buildArrowSchema(TapTable tapTable) {
        List<Field> fields = new ArrayList<>();
        
        // 处理空表情况
        if (tapTable.getNameFieldMap() == null) {
            return new Schema(fields);
        }
        
        for (TapField tapField : tapTable.getNameFieldMap().values()) {
            String fieldName = tapField.getName();
            org.apache.arrow.vector.types.pojo.ArrowType arrowType = TypeConverter.fromTapField(tapField);
            boolean nullable = tapField.getNullable() == null || tapField.getNullable();
            
            // 根据 nullable 属性创建字段类型
            FieldType fieldType = new FieldType(nullable, arrowType, null);
            Field field = new Field(fieldName, fieldType, null);
            fields.add(field);
        }
        
        logger.debug("Built Arrow Schema with {} fields", fields.size());
        return new Schema(fields);
    }
    
    /**
     * 将TapTableDto转换为Arrow Schema（优先使用预计算类型）
     */
    public Schema buildArrowSchema(TapTableDto tapTableDto) {
        List<Field> fields = new ArrayList<>();
        
        // 处理空表情况
        if (tapTableDto.getFields() == null) {
            return new Schema(fields);
        }
        
        for (TapFieldDto tapFieldDto : tapTableDto.getFields()) {
            String fieldName = tapFieldDto.getName();
            org.apache.arrow.vector.types.pojo.ArrowType arrowType = TypeConverter.fromTapFieldDto(tapFieldDto);
            boolean nullable = tapFieldDto.getNullable() == null || tapFieldDto.getNullable();
            
            // 根据 nullable 属性创建字段类型
            FieldType fieldType = new FieldType(nullable, arrowType, null);
            Field field = new Field(fieldName, fieldType, null);
            fields.add(field);
        }
        
        logger.debug("Built Arrow Schema from TapTableDto with {} fields", fields.size());
        return new Schema(fields);
    }

    /**
     * 构建Arrow VectorSchemaRoot
     */
    public VectorSchemaRoot createVectorSchemaRoot(List<Map<String, Object>> data, Schema schema) {
        // 创建 VectorSchemaRoot
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

        if (data.isEmpty()) {
            return root;
        }
        
        int rowCount = data.size();
        
        // 初始化向量
        root.setRowCount(rowCount);
        
        // 填充数据到向量
        for (int i = 0; i < schema.getFields().size(); i++) {
            Field field = schema.getFields().get(i);
            ValueVector vector = root.getVector(i);
            String fieldName = field.getName();
            
            for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
                Object value = data.get(rowIdx).get(fieldName);
                setVectorValue(vector, rowIdx, value);
            }
            
            vector.setValueCount(rowCount);
        }
        
        logger.debug("Created VectorSchemaRoot with {} rows and {} columns", rowCount, schema.getFields().size());
        return root;
    }

    /**
     * 设置向量值（简化版本）
     * 
     * @deprecated 使用 {@link ArrowValueHandler#setVectorValue(ValueVector, int, Object)} 替代
     */
    @Deprecated
    private void setVectorValue(ValueVector vector, int index, Object value) {
        ArrowValueHandler.setVectorValue(vector, index, value);
    }

    /**
     * 使用Arrow写入数据到表（优先零拷贝，降级到COPY模式）
     */
    public void writeWithArrow(List<Map<String, Object>> data, String tableName, TapTable tapTable) throws SQLException {
        if (data.isEmpty()) {
            return;
        }
        
        // 尝试零拷贝写入，如果不可用则使用COPY模式
        if (!tryZeroCopyWrite(data, tableName, tapTable)) {
            AppenderInsert(data, tableName, tapTable);
        }
    }

    /**
     * 尝试真正的零拷贝写入
     */
    private boolean tryZeroCopyWrite(List<Map<String, Object>> data, String tableName, TapTable tapTable) {
        if (!zeroCopyEnabled) {
            logger.debug("Arrow zero-copy is disabled, falling back to COPY mode");
            return false;
        }

        long startTime = System.currentTimeMillis();

        try {
            // 1. 构建 Arrow Schema
            Schema schema = buildArrowSchema(tapTable);

            // 2. 构建 VectorSchemaRoot
            try (VectorSchemaRoot root = createVectorSchemaRoot(data, schema);
                 ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                 ArrowStreamWriter writer = new ArrowStreamWriter(root, new DictionaryProvider.MapDictionaryProvider(), outputStream)) {
                // 3. 将 Arrow 数据写入 Arrow Stream
                writer.start();
                writer.writeBatch();
                writer.end();

                // 4. 将 Arrow Stream 导出为 ArrowArrayStream 并注册到 DuckDB
                byte[] arrowBytes = outputStream.toByteArray();
                DuckDBConnection duckDbConnection = connection.unwrap(DuckDBConnection.class);
                
                if (duckDbConnection == null) {
                    logger.debug("Connection is not a DuckDB connection, skipping zero-copy write");
                    return false;
                }
                
                String streamName;
                synchronized (ArrowWriter.class) {
                    streamName = "arrow_stream_" + (++tempTableCounter);
                }

                try (InputStream inputStream = new ByteArrayInputStream(arrowBytes);
                     ArrowStreamReader reader = new ArrowStreamReader(inputStream, allocator);
                     ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
                     Statement statement = connection.createStatement()) {
                    Data.exportArrayStream(allocator, reader, stream);
                    duckDbConnection.registerArrowStream(streamName, stream);
                    statement.executeUpdate("INSERT INTO " + tableName + " SELECT * FROM " + streamName);
                }

                long duration = System.currentTimeMillis() - startTime;
                logger.info("Successfully wrote {} rows using Arrow to table {} in {} ms", 
                    data.size(), tableName, duration);
                return true;
            }

        } catch (Exception e) {
            logger.warn("Arrow zero-copy write failed, falling back to COPY mode: {}", e.getMessage());
            logger.debug("Zero-copy failure details:", e);
            return false;
        }
    }

    /**
     * 使用DuckDB官方推荐的Appender API插入数据（最高性能）
     */
    private void AppenderInsert(List<Map<String, Object>> data, String tableName, TapTable tapTable) throws SQLException {
        if (data.isEmpty()) {
            return;
        }
        
        List<String> fieldNames = new ArrayList<>(tapTable.getNameFieldMap().keySet());
        if (fieldNames.isEmpty()) {
            return;
        }
        
        logger.debug("Starting Appender-based insert of {} rows into table {}", data.size(), tableName);
        
        try {
            connection.setAutoCommit(false);
            
            try {
                DuckDBConnection duckDbConnection = connection.unwrap(DuckDBConnection.class);
                
                if (duckDbConnection != null) {
                    try (DuckDBAppender appender = duckDbConnection.createAppender(DuckDBConnection.DEFAULT_SCHEMA, tableName)) {
                        for (Map<String, Object> row : data) {
                            appender.beginRow();
                            for (String fieldName : fieldNames) {
                                Object value = row.get(fieldName);
                                appendToAppender(appender, value);
                            }
                            appender.endRow();
                        }
                    }
                } else {
                    fallbackInsert(data, tableName, tapTable);
                }
                
                connection.commit();
                logger.debug("Successfully inserted {} rows into table {} using Appender", data.size(), tableName);
            } catch (SQLException e) {
                connection.rollback();
                logger.warn("Appender insert failed for table {}, falling back to INSERT: {}", tableName, e.getMessage());
                fallbackInsert(data, tableName, tapTable);
                connection.commit();
            }
        } finally {
            try {
                if (!connection.getAutoCommit()) {
                    connection.setAutoCommit(true);
                }
            } catch (SQLException e) {
                logger.debug("Failed to reset auto-commit", e);
            }
        }
        
        logger.debug("Inserted {} rows into table {} using Appender mode", data.size(), tableName);
    }

    /**
     * 将值追加到DuckDB Appender（处理类型转换）
     * 
     * @deprecated 使用 {@link ArrowValueHandler#appendToAppender(DuckDBAppender, Object)} 替代
     */
    @Deprecated
    private void appendToAppender(DuckDBAppender appender, Object value) throws SQLException {
        try {
            ArrowValueHandler.appendToAppender(appender, value);
        } catch (Exception e) {
            throw new SQLException("Failed to append value to DuckDB Appender: " + e.getMessage(), e);
        }
    }

    /**
     * 备用插入方法（当COPY不可用时使用）
     * 优化：添加 ON CONFLICT DO NOTHING 处理主键冲突，避免批量插入失败
     */
    private void fallbackInsert(List<Map<String, Object>> data, String tableName, TapTable tapTable) throws SQLException {
        List<String> fieldNames = new ArrayList<>(tapTable.getNameFieldMap().keySet());
        if (fieldNames.isEmpty() || data.isEmpty()) {
            return;
        }
        
        // 检查是否有主键
        List<String> primaryKeyCols = new ArrayList<>();
        for (io.tapdata.entity.schema.TapField tapField : tapTable.getNameFieldMap().values()) {
            if (Boolean.TRUE.equals(tapField.getPrimaryKey())) {
                primaryKeyCols.add(tapField.getName());
            }
        }
        
        String columns = fieldNames.stream()
                .map(f -> "\"" + f + "\"")
                .reduce((a, b) -> a + ", " + b)
                .orElse("");
        
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ").append(tableName).append(" (").append(columns).append(") VALUES ");
        
        List<String> valueGroups = new ArrayList<>();
        for (Map<String, Object> row : data) {
            List<String> values = new ArrayList<>();
            for (String fieldName : fieldNames) {
                Object value = row.get(fieldName);
                values.add(formatValueForSql(value));
            }
            valueGroups.add("(" + String.join(",", values) + ")");
        }
        
        sqlBuilder.append(String.join(", ", valueGroups));
        
        // 如果有主键，添加 ON CONFLICT DO NOTHING 避免主键冲突
        if (!primaryKeyCols.isEmpty()) {
            sqlBuilder.append(" ON CONFLICT (");
            sqlBuilder.append(primaryKeyCols.stream()
                    .map(f -> "\"" + f + "\"")
                    .reduce((a, b) -> a + ", " + b)
                    .orElse(""));
            sqlBuilder.append(") DO NOTHING");
            logger.debug("Added ON CONFLICT DO NOTHING for primary keys: {}", primaryKeyCols);
        }
        
        try (Statement stmt = connection.createStatement()) {
            int insertedRows = stmt.executeUpdate(sqlBuilder.toString());
            logger.warn("Inserted {} rows into table {} (some rows may be ignored due to primary key conflict)",
                insertedRows, tableName);
        } catch (SQLException e) {
            // 如果 ON CONFLICT 语法不支持，降级到逐行插入
            logger.warn("Batch insert failed, falling back to row-by-row insert: {}", e.getMessage());
            insertRowByRow(data, tableName, tapTable, fieldNames);
        }
    }
    
    /**
     * 逐行插入（用于处理批量插入失败的情况）
     */
    private void insertRowByRow(List<Map<String, Object>> data, String tableName, 
                                TapTable tapTable, List<String> fieldNames) throws SQLException {
        int successCount = 0;
        int ignoreCount = 0;
        
        for (Map<String, Object> row : data) {
            try {
                StringBuilder singleSql = new StringBuilder();
                singleSql.append("INSERT INTO ").append(tableName).append(" (");
                singleSql.append(fieldNames.stream()
                        .map(f -> "\"" + f + "\"")
                        .reduce((a, b) -> a + ", " + b)
                        .orElse(""));
                singleSql.append(") VALUES (");
                
                List<String> values = new ArrayList<>();
                for (String fieldName : fieldNames) {
                    Object value = row.get(fieldName);
                    values.add(formatValueForSql(value));
                }
                singleSql.append(String.join(",", values));
                singleSql.append(")");
                
                try (Statement stmt = connection.createStatement()) {
                    stmt.executeUpdate(singleSql.toString());
                    successCount++;
                }
            } catch (SQLException e) {
                // 忽略主键冲突等约束错误，继续处理其他行
                if (e.getMessage() != null && e.getMessage().contains("primary key constraint")) {
                    ignoreCount++;
                    logger.debug("Ignored duplicate key for row: {}", row);
                } else {
                    logger.warn("Failed to insert row: {}", e.getMessage());
                    throw e;
                }
            }
        }
        
        logger.info("Row-by-row insert completed: {} succeeded, {} ignored (duplicate key)", 
            successCount, ignoreCount);
    }

    /**
     * 格式化值为CSV格式
     */
    private String formatValue(Object value) {
        if (value == null) {
            return "";
        }
        if (value instanceof String) {
            String str = (String) value;
            str = str.replace("\"", "\"\"");
            if (str.contains(",") || str.contains("\"") || str.contains("\n")) {
                return "\"" + str + "\"";
            }
            return str;
        }
        if (value instanceof java.util.Date) {
            return new java.sql.Timestamp(((java.util.Date) value).getTime()).toString();
        }
        return value.toString();
    }

    /**
     * 格式化值为SQL格式
     * 
     * @deprecated 使用 {@link DuckDbSqlValueFormatter#format(Object)} 替代
     */
    @Deprecated
    private String formatValueForSql(Object value) {
        return DuckDbSqlValueFormatter.format(value);
    }

    /**
     * 使用Arrow写入数据到表（支持普通DuckDB表或DuckLake表）
     */
    public void writeWithArrow(List<Map<String, Object>> data, String tableName, TapTable tapTable, boolean useDuckLake) throws SQLException {
        if (useDuckLake && duckLakeConfig.isEnabled()) {
            writeToDuckLake(data, tableName, tapTable);
        } else {
            writeWithArrow(data, tableName, tapTable);
        }
    }
    
    /**
     * 使用Arrow写入数据到表（使用TapTableDto）
     */
    public void writeWithArrow(List<Map<String, Object>> data, String tableName, TapTableDto tapTableDto) throws SQLException {
        if (data.isEmpty()) {
            return;
        }
        
        // 尝试零拷贝写入，如果不可用则使用COPY模式
        if (!tryZeroCopyWrite(data, tableName, tapTableDto)) {
            // 转换为TapTable后使用AppenderInsert
            TapTable tapTable = convertToTapTable(tapTableDto);
            AppenderInsert(data, tableName, tapTable);
        }
    }
    
    /**
     * 使用Arrow写入数据到表（使用TapTableDto，支持普通DuckDB表或DuckLake表）
     */
    public void writeWithArrow(List<Map<String, Object>> data, String tableName, TapTableDto tapTableDto, boolean useDuckLake) throws SQLException {
        if (useDuckLake && duckLakeConfig.isEnabled()) {
            writeToDuckLake(data, tableName, tapTableDto);
        } else {
            writeWithArrow(data, tableName, tapTableDto);
        }
    }

    /**
     * 使用Arrow写入数据到表（使用NodeSchemaInfo，优先使用预计算Schema）
     */
    public void writeWithArrow(List<Map<String, Object>> data, NodeSchemaInfo schemaInfo) throws SQLException {
        if (data.isEmpty()) {
            return;
        }
        
        String targetTableName = schemaInfo.getTargetTableName();
        
        // 尝试零拷贝写入，如果不可用则使用COPY模式
        if (!tryZeroCopyWrite(data, schemaInfo)) {
            // 使用SchemaInfo中的TapTable
            AppenderInsert(data, targetTableName, schemaInfo.getTapTable());
        }
    }

    /**
     * 使用Arrow写入数据到表（使用NodeSchemaInfo，支持普通DuckDB表或DuckLake表）
     */
    public void writeWithArrow(List<Map<String, Object>> data, NodeSchemaInfo schemaInfo, boolean useDuckLake) throws SQLException {
        if (useDuckLake && duckLakeConfig.isEnabled()) {
            writeToDuckLake(data, schemaInfo);
        } else {
            writeWithArrow(data, schemaInfo);
        }
    }
    
    /**
     * 写入数据到DuckLake表
     */
    private void writeToDuckLake(List<Map<String, Object>> data, String tableName, TapTable tapTable) throws SQLException {
        if (data.isEmpty()) {
            return;
        }
        
        long startTime = System.currentTimeMillis();
        
        // 1. 确保DuckLake表存在
        createDuckLakeTableIfNotExists(tableName, tapTable);
        
        // 2. 使用Arrow写入数据
        writeWithArrow(data, tableName, tapTable);
        
        long duration = System.currentTimeMillis() - startTime;
        logger.info("Successfully wrote {} rows to DuckLake table {} in {} ms", 
            data.size(), tableName, duration);
    }
    
    /**
     * 写入数据到DuckLake表（使用TapTableDto）
     */
    private void writeToDuckLake(List<Map<String, Object>> data, String tableName, TapTableDto tapTableDto) throws SQLException {
        if (data.isEmpty()) {
            return;
        }
        
        long startTime = System.currentTimeMillis();
        
        // 1. 确保DuckLake表存在
        createDuckLakeTableIfNotExists(tableName, tapTableDto);
        
        // 2. 使用Arrow写入数据
        writeWithArrow(data, tableName, tapTableDto);
        
        long duration = System.currentTimeMillis() - startTime;
        logger.info("Successfully wrote {} rows to DuckLake table {} in {} ms", 
            data.size(), tableName, duration);
    }

    /**
     * 写入数据到DuckLake表（使用NodeSchemaInfo）
     */
    private void writeToDuckLake(List<Map<String, Object>> data, NodeSchemaInfo schemaInfo) throws SQLException {
        if (data.isEmpty()) {
            return;
        }
        
        long startTime = System.currentTimeMillis();
        String targetTableName = schemaInfo.getTargetTableName();
        
        // 1. 确保DuckLake表存在
        createDuckLakeTableIfNotExists(schemaInfo);
        
        // 2. 使用Arrow写入数据
        writeWithArrow(data, schemaInfo);
        
        long duration = System.currentTimeMillis() - startTime;
        logger.info("Successfully wrote {} rows to DuckLake table {} in {} ms", 
            data.size(), targetTableName, duration);
    }
    
    /**
     * 尝试真正的零拷贝写入（使用TapTableDto）
     */
    private boolean tryZeroCopyWrite(List<Map<String, Object>> data, String tableName, TapTableDto tapTableDto) {
        if (!zeroCopyEnabled) {
            logger.debug("Arrow zero-copy is disabled, falling back to COPY mode");
            return false;
        }

        long startTime = System.currentTimeMillis();

        try {
            // 1. 构建 Arrow Schema（使用预计算类型）
            Schema schema = buildArrowSchema(tapTableDto);

            // 2. 构建 VectorSchemaRoot
            try (VectorSchemaRoot root = createVectorSchemaRoot(data, schema);
                 ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                 ArrowStreamWriter writer = new ArrowStreamWriter(root, new DictionaryProvider.MapDictionaryProvider(), outputStream)) {
                // 3. 将 Arrow 数据写入 Arrow Stream
                writer.start();
                writer.writeBatch();
                writer.end();

                // 4. 将 Arrow Stream 导出为 ArrowArrayStream 并注册到 DuckDB
                byte[] arrowBytes = outputStream.toByteArray();
                DuckDBConnection duckDbConnection = connection.unwrap(DuckDBConnection.class);
                
                if (duckDbConnection == null) {
                    logger.debug("Connection is not a DuckDB connection, skipping zero-copy write");
                    return false;
                }
                
                String streamName;
                synchronized (ArrowWriter.class) {
                    streamName = "arrow_stream_" + (++tempTableCounter);
                }

                try (InputStream inputStream = new ByteArrayInputStream(arrowBytes);
                     ArrowStreamReader reader = new ArrowStreamReader(inputStream, allocator);
                     ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
                     Statement statement = connection.createStatement()) {
                    Data.exportArrayStream(allocator, reader, stream);
                    duckDbConnection.registerArrowStream(streamName, stream);
                    statement.executeUpdate("INSERT INTO " + tableName + " SELECT * FROM " + streamName);
                }

                long duration = System.currentTimeMillis() - startTime;
                logger.info("Successfully wrote {} rows using Arrow from TapTableDto to table {} in {} ms", 
                    data.size(), tableName, duration);
                return true;
            }

        } catch (Exception e) {
            logger.warn("Arrow zero-copy write failed, falling back to COPY mode: {}", e.getMessage());
            logger.debug("Zero-copy failure details:", e);
            return false;
        }
    }

    /**
     * 尝试真正的零拷贝写入（使用NodeSchemaInfo，优先使用预计算Schema）
     */
    private boolean tryZeroCopyWrite(List<Map<String, Object>> data, NodeSchemaInfo schemaInfo) {
        if (!zeroCopyEnabled) {
            logger.debug("Arrow zero-copy is disabled, falling back to COPY mode");
            return false;
        }

        long startTime = System.currentTimeMillis();
        String targetTableName = schemaInfo.getTargetTableName();

        try {
            // 1. 使用预计算的 Arrow Schema
            Schema schema = schemaInfo.getArrowSchema();

            // 2. 构建 VectorSchemaRoot
            try (VectorSchemaRoot root = createVectorSchemaRoot(data, schema);
                 ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                 ArrowStreamWriter writer = new ArrowStreamWriter(root, new DictionaryProvider.MapDictionaryProvider(), outputStream)) {
                // 3. 将 Arrow 数据写入 Arrow Stream
                writer.start();
                writer.writeBatch();
                writer.end();

                // 4. 将 Arrow Stream 导出为 ArrowArrayStream 并注册到 DuckDB
                byte[] arrowBytes = outputStream.toByteArray();
                DuckDBConnection duckDbConnection = connection.unwrap(DuckDBConnection.class);
                
                if (duckDbConnection == null) {
                    logger.debug("Connection is not a DuckDB connection, skipping zero-copy write");
                    return false;
                }
                
                String streamName;
                synchronized (ArrowWriter.class) {
                    streamName = "arrow_stream_" + (++tempTableCounter);
                }

                try (InputStream inputStream = new ByteArrayInputStream(arrowBytes);
                     ArrowStreamReader reader = new ArrowStreamReader(inputStream, allocator);
                     ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
                     Statement statement = connection.createStatement()) {
                    Data.exportArrayStream(allocator, reader, stream);
                    duckDbConnection.registerArrowStream(streamName, stream);
                    statement.executeUpdate("INSERT INTO " + targetTableName + " SELECT * FROM " + streamName);
                }

                long duration = System.currentTimeMillis() - startTime;
                logger.info("Successfully wrote {} rows using Arrow from NodeSchemaInfo to table {} in {} ms", 
                    data.size(), targetTableName, duration);
                return true;
            }

        } catch (Exception e) {
            logger.warn("Arrow zero-copy write failed, falling back to COPY mode: {}", e.getMessage());
            logger.debug("Zero-copy failure details:", e);
            return false;
        }
    }
    
    /**
     * 确保DuckLake表存在，不存在则创建
     */
    private void createDuckLakeTableIfNotExists(String tableName, TapTable tapTable) throws SQLException {
        createDuckLakeTableIfNotExists(tableName, tapTable, false);
    }

    /**
     * 确保DuckLake表存在，不存在则创建
     * @param tableName 表名
     * @param tapTable 表结构
     * @param useTempTable 是否使用临时表
     */
    private void createDuckLakeTableIfNotExists(String tableName, TapTable tapTable, boolean useTempTable) throws SQLException {
        String createTableSql = buildDuckLakeCreateTableSql(tableName, tapTable, useTempTable);
        
        try (java.sql.Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);
            logger.debug("Successfully created/verified DuckLake table: {}", tableName);
        }
    }
    
    /**
     * 确保DuckLake表存在，不存在则创建（使用TapTableDto）
     */
    private void createDuckLakeTableIfNotExists(String tableName, TapTableDto tapTableDto) throws SQLException {
        createDuckLakeTableIfNotExists(tableName, tapTableDto, false);
    }

    /**
     * 确保DuckLake表存在，不存在则创建（使用TapTableDto）
     * @param tableName 表名
     * @param tapTableDto 表结构DTO
     * @param useTempTable 是否使用临时表
     */
    private void createDuckLakeTableIfNotExists(String tableName, TapTableDto tapTableDto, boolean useTempTable) throws SQLException {
        String createTableSql = buildDuckLakeCreateTableSql(tableName, tapTableDto, useTempTable);
        
        try (java.sql.Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);
            logger.debug("Successfully created/verified DuckLake table from DTO: {}", tableName);
        }
    }

    /**
     * 确保DuckLake表存在，不存在则创建（使用NodeSchemaInfo）
     */
    private void createDuckLakeTableIfNotExists(NodeSchemaInfo schemaInfo) throws SQLException {
        createDuckLakeTableIfNotExists(schemaInfo, false);
    }

    /**
     * 确保DuckLake表存在，不存在则创建（使用NodeSchemaInfo）
     * @param schemaInfo 预加载的Schema信息
     * @param useTempTable 是否使用临时表
     */
    private void createDuckLakeTableIfNotExists(NodeSchemaInfo schemaInfo, boolean useTempTable) throws SQLException {
        String targetTableName = schemaInfo.getTargetTableName();
        String createTableSql = buildDuckLakeCreateTableSql(schemaInfo, useTempTable);
        
        try (java.sql.Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);
            logger.debug("Successfully created/verified DuckLake table from NodeSchemaInfo: {}", targetTableName);
        }
    }
    
    /**
     * 将TapTableDto转换为TapTable（用于需要TapTable对象的场景）
     */
    private TapTable convertToTapTable(TapTableDto tapTableDto) {
        TapTable tapTable = new TapTable();
        tapTable.setId(tapTableDto.getId());
        tapTable.setName(tapTableDto.getName());
        
        if (tapTableDto.getFields() != null) {
            java.util.LinkedHashMap<String, TapField> nameFieldMap = new java.util.LinkedHashMap<>();
            for (TapFieldDto fieldDto : tapTableDto.getFields()) {
                TapField tapField = new TapField(fieldDto.getName(), fieldDto.getDataType());
                tapField.setPrimaryKey(Boolean.TRUE.equals(fieldDto.getIsPrimaryKey()));
                if (fieldDto.getPrimaryKeyPos() != null && fieldDto.getPrimaryKeyPos() > 0) {
                    tapField.setPrimaryKeyPos(fieldDto.getPrimaryKeyPos());
                }
                tapField.setNullable(fieldDto.getNullable() != null ? fieldDto.getNullable() : true);
                if (fieldDto.getPos() != null && fieldDto.getPos() > 0) {
                    tapField.setPos(fieldDto.getPos());
                }
                nameFieldMap.put(tapField.getName(), tapField);
            }
            tapTable.setNameFieldMap(nameFieldMap);
        }
        
        return tapTable;
    }
    
    /**
     * 构建DuckLake表的CREATE TABLE语句
     */
    private String buildDuckLakeCreateTableSql(String tableName, TapTable tapTable) {
        return buildDuckLakeCreateTableSql(tableName, tapTable, false);
    }

    /**
     * 构建DuckLake表的CREATE TABLE语句
     * @param tableName 表名
     * @param tapTable 表结构
     * @param useTempTable 是否使用临时表
     */
    private String buildDuckLakeCreateTableSql(String tableName, TapTable tapTable, boolean useTempTable) {
        StringBuilder sql = new StringBuilder();
        if (useTempTable) {
            sql.append("CREATE TEMP TABLE IF NOT EXISTS ").append(tableName).append(" (\n");
        } else {
            sql.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (\n");
        }
        
        java.util.List<String> columnDefs = new java.util.ArrayList<>();
        java.util.List<String> primaryKeyCols = new java.util.ArrayList<>();
        
        for (io.tapdata.entity.schema.TapField tapField : tapTable.getNameFieldMap().values()) {
            String colDef = buildColumnDefinition(tapField);
            columnDefs.add(colDef);
            
            if (Boolean.TRUE.equals(tapField.getPrimaryKey())) {
                primaryKeyCols.add(tapField.getName());
            }
        }
        
        sql.append(String.join(",\n", columnDefs));
        
        // 主键定义
        if (!primaryKeyCols.isEmpty()) {
            sql.append(",\nPRIMARY KEY (").append(String.join(",", primaryKeyCols)).append(")");
        }
        
        sql.append("\n) WITH (\n    SNAPSHOTS = TRUE");
        
        // 存储配置
        if (duckLakeConfig != null && duckLakeConfig.isEnabled()) {
            if (duckLakeConfig.isS3Storage()) {
                sql.append(",\n    S3 = '").append(escapeSqlString(duckLakeConfig.getStoragePath())).append("'");
            } else if (duckLakeConfig.isLocalStorage()) {
                sql.append(",\n    LOCAL = '").append(escapeSqlString(duckLakeConfig.getStoragePath())).append("'");
            }
        }
        
        sql.append("\n);");
        return sql.toString();
    }
    
    /**
     * 构建DuckLake表的CREATE TABLE语句（使用TapTableDto）
     */
    private String buildDuckLakeCreateTableSql(String tableName, TapTableDto tapTableDto) {
        return buildDuckLakeCreateTableSql(tableName, tapTableDto, false);
    }

    /**
     * 构建DuckLake表的CREATE TABLE语句（使用TapTableDto）
     * @param tableName 表名
     * @param tapTableDto 表结构DTO
     * @param useTempTable 是否使用临时表
     */
    private String buildDuckLakeCreateTableSql(String tableName, TapTableDto tapTableDto, boolean useTempTable) {
        StringBuilder sql = new StringBuilder();
        if (useTempTable) {
            sql.append("CREATE TEMP TABLE IF NOT EXISTS ").append(tableName).append(" (\n");
        } else {
            sql.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (\n");
        }
        
        java.util.List<String> columnDefs = new java.util.ArrayList<>();
        java.util.List<String> primaryKeyCols = new java.util.ArrayList<>();
        
        for (TapFieldDto tapFieldDto : tapTableDto.getFields()) {
            String colDef = buildColumnDefinition(tapFieldDto);
            columnDefs.add(colDef);
            
            if (Boolean.TRUE.equals(tapFieldDto.getIsPrimaryKey())) {
                primaryKeyCols.add(tapFieldDto.getName());
            }
        }
        
        sql.append(String.join(",\n", columnDefs));
        
        // 主键定义
        if (!primaryKeyCols.isEmpty()) {
            sql.append(",\nPRIMARY KEY (").append(String.join(",", primaryKeyCols)).append(")");
        }
        
        sql.append("\n) WITH (\n    SNAPSHOTS = TRUE");
        
        // 存储配置
        if (duckLakeConfig != null && duckLakeConfig.isEnabled()) {
            if (duckLakeConfig.isS3Storage()) {
                sql.append(",\n    S3 = '").append(escapeSqlString(duckLakeConfig.getStoragePath())).append("'");
            } else if (duckLakeConfig.isLocalStorage()) {
                sql.append(",\n    LOCAL = '").append(escapeSqlString(duckLakeConfig.getStoragePath())).append("'");
            }
        }
        
        sql.append("\n);");
        return sql.toString();
    }
    
    /**
     * 构建列定义
     */
    private String buildColumnDefinition(io.tapdata.entity.schema.TapField tapField) {
        StringBuilder colDef = new StringBuilder();
        colDef.append("\"").append(tapField.getName()).append("\" ");
        
        // 使用 TypeConverter 进行类型映射
        String duckDbType = TypeConverter.getDuckDbType(tapField);
        colDef.append(duckDbType);
        
        // 可选：可空性
        if (tapField.getNullable() != null && !tapField.getNullable()) {
            colDef.append(" NOT NULL");
        }
        
        return colDef.toString();
    }
    
    /**
     * 构建列定义（使用TapFieldDto预计算类型）
     */
    private String buildColumnDefinition(TapFieldDto tapFieldDto) {
        StringBuilder colDef = new StringBuilder();
        colDef.append("\"").append(tapFieldDto.getName()).append("\" ");
        
        // 使用 TypeConverter 进行类型映射（优先使用预计算类型）
        String duckDbType = TypeConverter.getDuckDbTypeFromDto(tapFieldDto);
        colDef.append(duckDbType);
        
        // 可选：可空性
        if (tapFieldDto.getNullable() != null && !tapFieldDto.getNullable()) {
            colDef.append(" NOT NULL");
        }
        
        return colDef.toString();
    }

    /**
     * 构建DuckLake表的CREATE TABLE语句（使用NodeSchemaInfo）
     */
    private String buildDuckLakeCreateTableSql(NodeSchemaInfo schemaInfo, boolean useTempTable) {
        String targetTableName = schemaInfo.getTargetTableName();
        StringBuilder sql = new StringBuilder();
        if (useTempTable) {
            sql.append("CREATE TEMP TABLE IF NOT EXISTS ").append(targetTableName).append(" (\n");
        } else {
            sql.append("CREATE TABLE IF NOT EXISTS ").append(targetTableName).append(" (\n");
        }
        
        java.util.List<String> columnDefs = new java.util.ArrayList<>();
        java.util.List<String> primaryKeyCols = schemaInfo.getPrimaryKeys();
        
        for (TapField tapField : schemaInfo.getFieldMap().values()) {
            String colDef = buildColumnDefinition(tapField);
            columnDefs.add(colDef);
        }
        
        sql.append(String.join(",\n", columnDefs));
        
        // 主键定义
        if (!primaryKeyCols.isEmpty()) {
            sql.append(",\nPRIMARY KEY (").append(String.join(",", primaryKeyCols)).append(")");
        }
        
        sql.append("\n) WITH (\n    SNAPSHOTS = TRUE");
        
        // 存储配置
        if (duckLakeConfig != null && duckLakeConfig.isEnabled()) {
            if (duckLakeConfig.isS3Storage()) {
                sql.append(",\n    S3 = '").append(escapeSqlString(duckLakeConfig.getStoragePath())).append("'");
            } else if (duckLakeConfig.isLocalStorage()) {
                sql.append(",\n    LOCAL = '").append(escapeSqlString(duckLakeConfig.getStoragePath())).append("'");
            }
        }
        
        sql.append("\n);");
        return sql.toString();
    }
    
    /**
     * 转义SQL字符串
     */
    private String escapeSqlString(String str) {
        if (str == null) {
            return "";
        }
        return str.replace("'", "''");
    }
    
    @Override
    public void close() {
        // 释放 Arrow 内存分配器
        if (allocator != null) {
            allocator.close();
            logger.debug("ArrowWriter closed, released BufferAllocator");
        }
    }
}
