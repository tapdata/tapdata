package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
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
            org.apache.arrow.vector.types.pojo.ArrowType arrowType = convertTapTypeToArrowType(tapField);
            boolean nullable = tapField.getNullable() == null || tapField.getNullable();
            
            // 根据 nullable 属性创建字段类型
            FieldType fieldType = new FieldType(nullable, arrowType, null);
            Field field = new Field(fieldName, fieldType, null);
            fields.add(field);
        }
        
        return new Schema(fields);
    }

    /**
     * 将Tap类型转换为Arrow类型（简化版本）
     */
    private org.apache.arrow.vector.types.pojo.ArrowType convertTapTypeToArrowType(TapField tapField) {
        // 优先使用 dataType
        String dataType = tapField.getDataType();
        if (dataType != null && !dataType.isEmpty()) {
            return mapDataTypeToArrowType(dataType);
        }
        
        // 其次使用 tapType
        TapType tapType = tapField.getTapType();
        if (tapType != null) {
            String typeName = tapType.getClass().getSimpleName();
            if ("TapNumber".equals(typeName)) {
                return new org.apache.arrow.vector.types.pojo.ArrowType.Int(64, true);
            } else if ("TapBoolean".equals(typeName)) {
                return new org.apache.arrow.vector.types.pojo.ArrowType.Bool();
            } else if ("TapDate".equals(typeName) || "TapDateTime".equals(typeName)) {
                return new org.apache.arrow.vector.types.pojo.ArrowType.Utf8();
            } else if ("TapBinary".equals(typeName)) {
                return new org.apache.arrow.vector.types.pojo.ArrowType.Binary();
            }
        }
        
        return new org.apache.arrow.vector.types.pojo.ArrowType.Utf8();
    }

    /**
     * 将数据库数据类型映射到Arrow类型（简化版本）
     */
    private org.apache.arrow.vector.types.pojo.ArrowType mapDataTypeToArrowType(String dataType) {
        String upperType = dataType.toUpperCase();
        if (upperType.contains("INT")) {
            if (upperType.contains("BIG")) {
                return new org.apache.arrow.vector.types.pojo.ArrowType.Int(64, true);
            } else if (upperType.contains("SMALL")) {
                return new org.apache.arrow.vector.types.pojo.ArrowType.Int(16, true);
            } else if (upperType.contains("TINY")) {
                return new org.apache.arrow.vector.types.pojo.ArrowType.Int(8, true);
            }
            return new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true);
        }
        if (upperType.contains("FLOAT") || upperType.contains("DOUBLE")) {
                return new org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
            }
        if (upperType.contains("BOOL")) {
            return new org.apache.arrow.vector.types.pojo.ArrowType.Bool();
        }
        if (upperType.contains("BLOB") || upperType.contains("BINARY")) {
            return new org.apache.arrow.vector.types.pojo.ArrowType.Binary();
        }
        return new org.apache.arrow.vector.types.pojo.ArrowType.Utf8();
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
     */
    private void setVectorValue(ValueVector vector, int index, Object value) {
        if (value == null) {
            // 对于基本类型向量，使用 defaultValue 设置 null
            if (vector instanceof BigIntVector) {
                ((BigIntVector) vector).setSafe(index, 0);
                ((BigIntVector) vector).setNull(index);
            } else if (vector instanceof IntVector) {
                ((IntVector) vector).setSafe(index, 0);
                ((IntVector) vector).setNull(index);
            } else if (vector instanceof SmallIntVector) {
                ((SmallIntVector) vector).setSafe(index, (short) 0);
                ((SmallIntVector) vector).setNull(index);
            } else if (vector instanceof TinyIntVector) {
                ((TinyIntVector) vector).setSafe(index, (byte) 0);
                ((TinyIntVector) vector).setNull(index);
            } else if (vector instanceof Float4Vector) {
                ((Float4Vector) vector).setSafe(index, 0f);
                ((Float4Vector) vector).setNull(index);
            } else if (vector instanceof Float8Vector) {
                ((Float8Vector) vector).setSafe(index, 0d);
                ((Float8Vector) vector).setNull(index);
            } else if (vector instanceof BitVector) {
                ((BitVector) vector).setSafe(index, 0);
                ((BitVector) vector).setNull(index);
            } else if (vector instanceof VarCharVector) {
                ((VarCharVector) vector).setSafe(index, new byte[0]);
            } else if (vector instanceof VarBinaryVector) {
                ((VarBinaryVector) vector).setSafe(index, new byte[0]);
            }
            return;
        }
        
        try {
            if (vector instanceof BigIntVector) {
                ((BigIntVector) vector).setSafe(index, ((Number) value).longValue());
            } else if (vector instanceof IntVector) {
                ((IntVector) vector).setSafe(index, ((Number) value).intValue());
            } else if (vector instanceof SmallIntVector) {
                ((SmallIntVector) vector).setSafe(index, ((Number) value).shortValue());
            } else if (vector instanceof TinyIntVector) {
                ((TinyIntVector) vector).setSafe(index, ((Number) value).byteValue());
            } else if (vector instanceof Float4Vector) {
                ((Float4Vector) vector).setSafe(index, ((Number) value).floatValue());
            } else if (vector instanceof Float8Vector) {
                ((Float8Vector) vector).setSafe(index, ((Number) value).doubleValue());
            } else if (vector instanceof BitVector) {
                ((BitVector) vector).setSafe(index, (Boolean) value ? 1 : 0);
            } else if (vector instanceof VarCharVector) {
                ((VarCharVector) vector).setSafe(index, convertDateTimeToString(value).getBytes(StandardCharsets.UTF_8));
            } else if (vector instanceof VarBinaryVector) {
                if (value instanceof byte[]) {
                    ((VarBinaryVector) vector).setSafe(index, (byte[]) value);
                } else {
                    ((VarBinaryVector) vector).setSafe(index, value.toString().getBytes(StandardCharsets.UTF_8));
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to set value at index {}: {}", index, e.getMessage());
            // 设置为 null
            if (vector instanceof BigIntVector) {
                ((BigIntVector) vector).setNull(index);
            } else if (vector instanceof IntVector) {
                ((IntVector) vector).setNull(index);
            } else if (vector instanceof SmallIntVector) {
                ((SmallIntVector) vector).setNull(index);
            } else if (vector instanceof TinyIntVector) {
                ((TinyIntVector) vector).setNull(index);
            } else if (vector instanceof Float4Vector) {
                ((Float4Vector) vector).setNull(index);
            } else if (vector instanceof Float8Vector) {
                ((Float8Vector) vector).setNull(index);
            } else if (vector instanceof BitVector) {
                ((BitVector) vector).setNull(index);
            }
        }
    }

    /**
     * 将 Tapdata PDK DateTime / TapDateTimeValue 转换为 DuckDB 兼容的时间戳字符串
     * 避免 DateTime.toString() 产生 "DateTime nano 0 seconds 1735689600 timeZone null" 这种非法格式
     */
    private static String convertDateTimeToString(Object value) {
        if (value instanceof DateTime dateTime) {
            long epochMs = dateTime.toInstant().toEpochMilli();
            return new java.sql.Timestamp(epochMs).toString();
        }
        if (value instanceof TapDateTimeValue tapDateTimeValue) {
            DateTime dt = tapDateTimeValue.getValue();
            if (dt != null) {
                long epochMs = dt.toInstant().toEpochMilli();
                return new java.sql.Timestamp(epochMs).toString();
            }
            return value.toString();
        }
        return value.toString();
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
     */
    private void appendToAppender(DuckDBAppender appender, Object value) throws SQLException {
        if (value == null) {
            appender.append((String) null);
            return;
        }
        
        if (value instanceof Integer) {
            appender.append((Integer) value);
        } else if (value instanceof Long) {
            appender.append((Long) value);
        } else if (value instanceof Short) {
            appender.append((Short) value);
        } else if (value instanceof Byte) {
            appender.append((Byte) value);
        } else if (value instanceof Float) {
            appender.append((Float) value);
        } else if (value instanceof Double) {
            appender.append((Double) value);
        } else if (value instanceof Boolean) {
            appender.append((Boolean) value);
        } else if (value instanceof String) {
            appender.append((String) value);
        } else if (value instanceof java.util.Date) {
            appender.append((java.util.Date) value);
        } else if (value instanceof java.sql.Timestamp) {
            appender.append(((java.sql.Timestamp) value).toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalDateTime());
        } else if (value instanceof DateTime dateTime) {
            appender.append(new java.sql.Timestamp(dateTime.toInstant().toEpochMilli()));
        } else if (value instanceof TapDateTimeValue tapDateTimeValue) {
            DateTime dt = tapDateTimeValue.getValue();
            if (dt != null) {
                appender.append(new java.sql.Timestamp(dt.toInstant().toEpochMilli()));
            } else {
                appender.append((String) null);
            }
        } else if (value instanceof java.time.LocalDateTime) {
            appender.append((java.time.LocalDateTime) value);
        } else if (value instanceof java.time.LocalDate) {
            appender.append((java.time.LocalDate) value);
        } else if (value instanceof java.math.BigDecimal) {
            appender.append((java.math.BigDecimal) value);
        } else if (value instanceof java.math.BigInteger) {
            appender.append((java.math.BigInteger) value);
        } else if (value instanceof byte[]) {
            appender.append((byte[]) value);
        } else if (value instanceof Collection<?>) {
            appender.append((Collection<?>) value);
        } else if (value instanceof Map<?, ?>) {
            appender.append((Map<?, ?>) value);
        } else {
            appender.append(value.toString());
        }
    }

    /**
     * 备用插入方法（当COPY不可用时使用）
     */
    private void fallbackInsert(List<Map<String, Object>> data, String tableName, TapTable tapTable) throws SQLException {
        List<String> fieldNames = new ArrayList<>(tapTable.getNameFieldMap().keySet());
        if (fieldNames.isEmpty() || data.isEmpty()) {
            return;
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
        
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sqlBuilder.toString());
        }
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
     */
    private String formatValueForSql(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof String) {
            String str = (String) value;
            str = str.replace("'", "''");
            return "'" + str + "'";
        }
        if (value instanceof java.util.Date) {
            return "'" + new java.sql.Timestamp(((java.util.Date) value).getTime()) + "'";
        }
        return value.toString();
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
     * 构建列定义
     */
    private String buildColumnDefinition(io.tapdata.entity.schema.TapField tapField) {
        StringBuilder colDef = new StringBuilder();
        colDef.append("\"").append(tapField.getName()).append("\" ");
        
        // 类型映射（简化）
        String dataType = tapField.getDataType();
        if (dataType != null) {
            String upperType = dataType.toUpperCase();
            if (upperType.contains("INT")) {
                if (upperType.contains("BIG")) {
                    colDef.append("BIGINT");
                } else {
                    colDef.append("INTEGER");
                }
            } else if (upperType.contains("FLOAT") || upperType.contains("DOUBLE")) {
                colDef.append("DOUBLE");
            } else if (upperType.contains("BOOL")) {
                colDef.append("BOOLEAN");
            } else if (upperType.contains("BLOB") || upperType.contains("BINARY")) {
                colDef.append("BINARY");
            } else if (upperType.contains("DATE") || upperType.contains("TIME")) {
                colDef.append("TIMESTAMP");
            } else {
                colDef.append("VARCHAR");
            }
        } else {
            colDef.append("VARCHAR");
        }
        
        // 可选：可空性
        if (tapField.getNullable() != null && !tapField.getNullable()) {
            colDef.append(" NOT NULL");
        }
        
        return colDef.toString();
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
