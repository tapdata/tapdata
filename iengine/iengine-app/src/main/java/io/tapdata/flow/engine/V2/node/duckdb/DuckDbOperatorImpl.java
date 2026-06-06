package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.dag.process.dto.TapFieldDto;
import com.tapdata.tm.commons.dag.process.dto.TapTableDto;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.flow.engine.V2.util.TapEventUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * DuckDB操作实现类 - 整合连接管理、Arrow写入和SQL执行
 */
public class DuckDbOperatorImpl implements DuckDbOperator {

    private static final Logger logger = LoggerFactory.getLogger(DuckDbOperatorImpl.class);

    private Connection connection;
    private ArrowWriter arrowWriter;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    // 批处理配置
    private final boolean batchWritingEnabled;
    private final int batchWritingSize;
    private final long batchWritingTimeoutMs;
    private final Collection<Map<String, Object>> batchBuffer = new ArrayList<>();
    private long lastFlushTime = System.currentTimeMillis();
    
    // DuckLake配置
    private final DuckLakeConfig duckLakeConfig;
    
    // 数据库文件路径（null=内存模式）
    private final String dbPath;

    /**
     * 创建内存数据库实例
     */
    public DuckDbOperatorImpl() throws SQLException {
        this("", false, 1000, 5000, DuckLakeConfig.disabled());
    }

    /**
     * 创建带批处理配置的实例
     * @param batchWritingEnabled 是否启用批处理
     * @param batchWritingSize 批处理大小
     * @param batchWritingTimeoutMs 批处理超时时间（毫秒）
     */
    public DuckDbOperatorImpl(boolean batchWritingEnabled, int batchWritingSize, long batchWritingTimeoutMs) throws SQLException {
        this("", batchWritingEnabled, batchWritingSize, batchWritingTimeoutMs, DuckLakeConfig.disabled());
    }

    /**
     * 创建带 dbPath 和批处理配置的实例
     * @param dbPath 数据库文件路径（null=内存模式）
     * @param batchWritingEnabled 是否启用批处理
     * @param batchWritingSize 批处理大小
     * @param batchWritingTimeoutMs 批处理超时时间（毫秒）
     */
    public DuckDbOperatorImpl(String dbPath, boolean batchWritingEnabled, int batchWritingSize, long batchWritingTimeoutMs) throws SQLException {
        this(dbPath, batchWritingEnabled, batchWritingSize, batchWritingTimeoutMs, DuckLakeConfig.disabled());
    }

    /**
     * 创建带批处理和DuckLake配置的实例（保留向后兼容）
     * @param batchWritingEnabled 是否启用批处理
     * @param batchWritingSize 批处理大小
     * @param batchWritingTimeoutMs 批处理超时时间（毫秒）
     * @param duckLakeConfig DuckLake配置
     */
    public DuckDbOperatorImpl(boolean batchWritingEnabled, int batchWritingSize, long batchWritingTimeoutMs, DuckLakeConfig duckLakeConfig) throws SQLException {
        this("", batchWritingEnabled, batchWritingSize, batchWritingTimeoutMs, duckLakeConfig);
    }

    /**
     * 创建带批处理、DuckLake和dbPath配置的实例（主构造函数）
     * @param dbPath 数据库文件路径（null=内存模式）
     * @param batchWritingEnabled 是否启用批处理
     * @param batchWritingSize 批处理大小
     * @param batchWritingTimeoutMs 批处理超时时间（毫秒）
     * @param duckLakeConfig DuckLake配置
     */
    public DuckDbOperatorImpl(String dbPath, boolean batchWritingEnabled, int batchWritingSize, long batchWritingTimeoutMs, DuckLakeConfig duckLakeConfig) throws SQLException {
        this.dbPath = dbPath;
        this.batchWritingEnabled = batchWritingEnabled;
        this.batchWritingSize = batchWritingSize;
        this.batchWritingTimeoutMs = batchWritingTimeoutMs;
        this.duckLakeConfig = duckLakeConfig;
        
        initConnection();
        this.arrowWriter = new ArrowWriter(connection, true, duckLakeConfig);
    }

    /**
     * 使用现有连接创建实例
     */
    public DuckDbOperatorImpl(Connection connection) {
        this(connection, true, 1000, 5000, DuckLakeConfig.disabled());
    }

    /**
     * 使用现有连接创建带批处理配置的实例
     */
    public DuckDbOperatorImpl(Connection connection, boolean batchWritingEnabled, int batchWritingSize, long batchWritingTimeoutMs) {
        this(connection, batchWritingEnabled, batchWritingSize, batchWritingTimeoutMs, DuckLakeConfig.disabled());
    }
    
    /**
     * 使用现有连接创建带批处理和DuckLake配置的实例
     */
    public DuckDbOperatorImpl(Connection connection, boolean batchWritingEnabled, int batchWritingSize, long batchWritingTimeoutMs, DuckLakeConfig duckLakeConfig) {
        this.connection = connection;
        this.dbPath = null; // 使用外部连接时，dbPath无意义
        this.batchWritingEnabled = batchWritingEnabled;
        this.batchWritingSize = batchWritingSize;
        this.batchWritingTimeoutMs = batchWritingTimeoutMs;
        this.duckLakeConfig = duckLakeConfig;
        this.arrowWriter = new ArrowWriter(connection, true, duckLakeConfig);
    }

    /**
     * 初始化DuckDB连接
     * 根据dbPath决定使用文件持久化模式还是内存模式
     */
    private void initConnection() throws SQLException {
        // 加载DuckDB驱动
        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("DuckDB driver not found", e);
        }
        
        // 根据dbPath动态构建JDBC URL
        String jdbcUrl;
        if (dbPath != null && !dbPath.trim().isEmpty()) {
            jdbcUrl = "jdbc:duckdb:" + dbPath.trim();
            logger.info("DuckDB using PERSISTENT file mode: {}", jdbcUrl);
        } else {
            jdbcUrl = "jdbc:duckdb:";
            logger.info("DuckDB using IN-MEMORY mode");
        }
        
        this.connection = DriverManager.getConnection(jdbcUrl);
        this.connection.setAutoCommit(true);
        
        logger.info("DuckDB connection initialized successfully");
    }

    @Override
    public List<Map<String, Object>> executeQuery(String sql) throws SQLException {
        checkClosed();
        
        List<Map<String, Object>> results = new ArrayList<>();
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            List<String> columnNames = new ArrayList<>();
            
            for (int i = 1; i <= columnCount; i++) {
                columnNames.add(metaData.getColumnName(i));
            }
            
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 0; i < columnCount; i++) {
                    row.put(columnNames.get(i), rs.getObject(i + 1));
                }
                results.add(row);
            }
        }
        
        return results;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        
        try (Statement stmt = connection.createStatement()) {
            return stmt.executeUpdate(sql);
        }
    }

    @Override
    public ExecuteResult execute(String sql) throws SQLException {
        checkClosed();
        
        try (Statement stmt = connection.createStatement()) {
            boolean hasResultSet = stmt.execute(sql);
            
            if (hasResultSet) {
                try (ResultSet rs = stmt.getResultSet()) {
                    List<Map<String, Object>> results = new ArrayList<>();
                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    List<String> columnNames = new ArrayList<>();
                    
                    for (int i = 1; i <= columnCount; i++) {
                        columnNames.add(metaData.getColumnName(i));
                    }
                    
                    while (rs.next()) {
                        Map<String, Object> row = new LinkedHashMap<>();
                        for (int i = 0; i < columnNames.size(); i++) {
                            row.put(columnNames.get(i), rs.getObject(i + 1));
                        }
                        results.add(row);
                    }
                    
                    return new ExecuteResult(true, results, 0);
                }
            } else {
                int updateCount = stmt.getUpdateCount();
                return new ExecuteResult(false, Collections.emptyList(), updateCount);
            }
        }
    }

    @Override
    public void writeBatch(List<Map<String, Object>> data, String tableName) throws SQLException, java.io.IOException {
        checkClosed();
        
        if (data == null || data.isEmpty()) {
            return;
        }
        
        // 获取TapTable用于Schema转换
        TapTable tapTable = inferTapTable(data, tableName);
        
        if (batchWritingEnabled) {
            synchronized (batchBuffer) {
                batchBuffer.addAll(data);
                lastFlushTime = System.currentTimeMillis();
                
                if (batchBuffer.size() >= batchWritingSize) {
                    flushBatch(tableName, tapTable);
                }
            }
        } else {
            writeWithArrow(data, tableName, tapTable);
        }
    }
    
    @Override
    public void writeBatch(List<Map<String, Object>> data, String tableName, TapTableDto tapTableDto) throws SQLException, java.io.IOException {
        checkClosed();
        
        if (data == null || data.isEmpty()) {
            return;
        }
        
        if (batchWritingEnabled) {
            synchronized (batchBuffer) {
                batchBuffer.addAll(data);
                lastFlushTime = System.currentTimeMillis();
                
                if (batchBuffer.size() >= batchWritingSize) {
                    flushBatch(tableName, tapTableDto);
                }
            }
        } else {
            writeWithArrow(data, tableName, tapTableDto);
        }
    }

    @Override
    public void writeBatch(List<Map<String, Object>> data, NodeSchemaInfo schemaInfo) throws SQLException, java.io.IOException {
        checkClosed();
        
        if (data == null || data.isEmpty()) {
            return;
        }
        
        String targetTableName = schemaInfo.getTargetTableName();
        
        if (batchWritingEnabled) {
            synchronized (batchBuffer) {
                batchBuffer.addAll(data);
                lastFlushTime = System.currentTimeMillis();
                
                if (batchBuffer.size() >= batchWritingSize) {
                    flushBatch(schemaInfo);
                }
            }
        } else {
            writeWithArrow(data, schemaInfo);
        }
    }

    /**
     * 刷新批处理缓冲区
     */
    public void flushBatch(String tableName, TapTable tapTable) throws SQLException, java.io.IOException {
        synchronized (batchBuffer) {
            if (batchBuffer.isEmpty()) {
                return;
            }
            
            List<Map<String, Object>> batchData = new ArrayList<>(batchBuffer);
            batchBuffer.clear();
            
            writeWithArrow(batchData, tableName, tapTable);
            logger.debug("Flushed {} records to table {}", batchData.size(), tableName);
        }
    }
    
    /**
     * 刷新批处理缓冲区（使用TapTableDto）
     */
    public void flushBatch(String tableName, TapTableDto tapTableDto) throws SQLException, java.io.IOException {
        synchronized (batchBuffer) {
            if (batchBuffer.isEmpty()) {
                return;
            }
            
            List<Map<String, Object>> batchData = new ArrayList<>(batchBuffer);
            batchBuffer.clear();
            
            writeWithArrow(batchData, tableName, tapTableDto);
            logger.debug("Flushed {} records to table {}", batchData.size(), tableName);
        }
    }

    @Override
    public void flushBatch(NodeSchemaInfo schemaInfo) throws SQLException, java.io.IOException {
        synchronized (batchBuffer) {
            if (batchBuffer.isEmpty()) {
                return;
            }
            
            List<Map<String, Object>> batchData = new ArrayList<>(batchBuffer);
            batchBuffer.clear();
            
            writeWithArrow(batchData, schemaInfo);
            logger.debug("Flushed {} records to table {}", batchData.size(), schemaInfo.getTargetTableName());
        }
    }
    
    /**
     * 使用Arrow写入数据（自动判断是否使用DuckLake）
     */
    private void writeWithArrow(List<Map<String, Object>> data, String tableName, TapTable tapTable) throws SQLException, java.io.IOException {
        if (duckLakeConfig.isEnabled()) {
            arrowWriter.writeWithArrow(data, tableName, tapTable, true);
        } else {
            arrowWriter.writeWithArrow(data, tableName, tapTable);
        }
    }
    
    /**
     * 使用Arrow写入数据（使用TapTableDto）
     */
    private void writeWithArrow(List<Map<String, Object>> data, String tableName, TapTableDto tapTableDto) throws SQLException, java.io.IOException {
        if (duckLakeConfig.isEnabled()) {
            arrowWriter.writeWithArrow(data, tableName, tapTableDto, true);
        } else {
            arrowWriter.writeWithArrow(data, tableName, tapTableDto);
        }
    }

    /**
     * 使用Arrow写入数据（使用NodeSchemaInfo，优先使用预计算Schema）
     */
    private void writeWithArrow(List<Map<String, Object>> data, NodeSchemaInfo schemaInfo) throws SQLException, java.io.IOException {
        String targetTableName = schemaInfo.getTargetTableName();
        if (duckLakeConfig.isEnabled()) {
            arrowWriter.writeWithArrow(data, schemaInfo, true);
        } else {
            arrowWriter.writeWithArrow(data, schemaInfo);
        }
    }

    /**
     * 从数据推断TapTable结构
     */
    private TapTable inferTapTable(List<Map<String, Object>> data, String tableName) {
        TapTable tapTable = new TapTable(tableName);
        
        if (data.isEmpty()) {
            return tapTable;
        }
        
        Map<String, Object> firstRow = data.get(0);
        
        for (Map.Entry<String, Object> entry : firstRow.entrySet()) {
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

    /**
     * 根据值推断TapType
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

    private TapType createTapString() {
        try {
            return (TapType) Class.forName("io.tapdata.entity.schema.type.TapString").getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            return null;
        }
    }

    private TapType createTapNumber() {
        try {
            return (TapType) Class.forName("io.tapdata.entity.schema.type.TapNumber").getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            return null;
        }
    }

    private TapType createTapBoolean() {
        try {
            return (TapType) Class.forName("io.tapdata.entity.schema.type.TapBoolean").getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            return null;
        }
    }

    private TapType createTapDate() {
        try {
            return (TapType) Class.forName("io.tapdata.entity.schema.type.TapDate").getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void createTable(TapTable tapTable) throws SQLException {
        createTable(tapTable, tapTable.getName(), false);
    }

    @Override
    public void createTable(TapTable tapTable, boolean useTempTable) throws SQLException {
        createTable(tapTable, tapTable.getName(), useTempTable);
    }

    @Override
    public void createTempTable(TapTable tapTable, String tempTableName) throws SQLException {
        createTable(tapTable, tempTableName, false);
    }

    @Override
    public void createTempTable(TapTable tapTable, String tempTableName, boolean useTempTable) throws SQLException {
        createTable(tapTable, tempTableName, useTempTable);
    }

    @Override
    public void createTempTable(NodeSchemaInfo schemaInfo) throws SQLException {
        createTempTable(schemaInfo, true);
    }

    @Override
    public void createTempTable(NodeSchemaInfo schemaInfo, boolean useTempTable) throws SQLException {
        checkClosed();
        
        String targetTableName = schemaInfo.getTargetTableName();
        
        StringBuilder sql = new StringBuilder();
        if (useTempTable) {
            sql.append("CREATE TEMP TABLE IF NOT EXISTS ").append(targetTableName).append(" (");
        } else {
            sql.append("CREATE TABLE IF NOT EXISTS ").append(targetTableName).append(" (");
        }
        
        List<String> fieldDefs = new ArrayList<>();
        
        for (TapField tapField : schemaInfo.getFieldMap().values()) {
            String fieldName = tapField.getName();
            String duckDbType = convertTapTypeToDuckDbType(tapField);
            
            StringBuilder fieldDef = new StringBuilder();
            fieldDef.append("\"").append(fieldName).append("\" ").append(duckDbType);
            
            if (schemaInfo.getPrimaryKeys().contains(fieldName)) {
                fieldDef.append(" PRIMARY KEY");
            }
            
            fieldDefs.add(fieldDef.toString());
        }
        
        sql.append(String.join(", ", fieldDefs));
        sql.append(")");
        
        executeUpdate(sql.toString());
        logger.debug("Created temporary table from NodeSchemaInfo: {}", targetTableName);
    }

    /**
     * 创建表
     */
    private void createTable(TapTable tapTable, String tableName, boolean useTempTable) throws SQLException {
        checkClosed();
        
        StringBuilder sql = new StringBuilder();
        if (useTempTable) {
            sql.append("CREATE TEMP TABLE IF NOT EXISTS ").append(tableName).append(" (");
        } else {
            sql.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");
        }
        
        List<String> fieldDefs = new ArrayList<>();
        
        for (TapField tapField : tapTable.getNameFieldMap().values()) {
            String fieldName = tapField.getName();
            String duckDbType = convertTapTypeToDuckDbType(tapField);
            
            StringBuilder fieldDef = new StringBuilder();
            fieldDef.append("\"").append(fieldName).append("\" ").append(duckDbType);
            
            if (Boolean.TRUE.equals(tapField.getPrimaryKey())) {
                fieldDef.append(" PRIMARY KEY");
            }
            
            fieldDefs.add(fieldDef.toString());
        }
        
        sql.append(String.join(", ", fieldDefs));
        sql.append(")");
        
        executeUpdate(sql.toString());
        logger.debug("Created table: {}", tableName);
    }
    
    /**
     * 创建表（使用TapTableDto）
     */
    @Override
    public void createTable(TapTableDto tapTableDto) throws SQLException {
        createTable(tapTableDto, tapTableDto.getName(), false);
    }
    
    /**
     * 创建表（使用TapTableDto）
     */
    private void createTable(TapTableDto tapTableDto, String tableName, boolean useTempTable) throws SQLException {
        checkClosed();
        
        StringBuilder sql = new StringBuilder();
        if (useTempTable) {
            sql.append("CREATE TEMP TABLE IF NOT EXISTS ").append(tableName).append(" (");
        } else {
            sql.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");
        }
        
        List<String> fieldDefs = new ArrayList<>();
        
        if (tapTableDto.getFields() != null) {
            for (TapFieldDto tapFieldDto : tapTableDto.getFields()) {
                String fieldName = tapFieldDto.getName();
                String duckDbType = TypeConverter.getDuckDbTypeFromDto(tapFieldDto);
                
                StringBuilder fieldDef = new StringBuilder();
                fieldDef.append("\"").append(fieldName).append("\" ").append(duckDbType);
                
                if (Boolean.TRUE.equals(tapFieldDto.getIsPrimaryKey())) {
                    fieldDef.append(" PRIMARY KEY");
                }
                
                fieldDefs.add(fieldDef.toString());
            }
        }
        
        sql.append(String.join(", ", fieldDefs));
        sql.append(")");
        
        executeUpdate(sql.toString());
        logger.debug("Created table: {} from TapTableDto", tableName);
    }

    @Override
    public void createTable(NodeSchemaInfo schemaInfo) throws SQLException {
        String targetTableName = schemaInfo.getTargetTableName();
        createTable(schemaInfo, targetTableName, false);
    }

    /**
     * 创建表（使用NodeSchemaInfo）
     */
    private void createTable(NodeSchemaInfo schemaInfo, String tableName, boolean useTempTable) throws SQLException {
        checkClosed();
        
        StringBuilder sql = new StringBuilder();
        if (useTempTable) {
            sql.append("CREATE TEMP TABLE IF NOT EXISTS ").append(tableName).append(" (");
        } else {
            sql.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");
        }
        
        List<String> fieldDefs = new ArrayList<>();
        
        for (TapField tapField : schemaInfo.getFieldMap().values()) {
            String fieldName = tapField.getName();
            String duckDbType = convertTapTypeToDuckDbType(tapField);
            
            StringBuilder fieldDef = new StringBuilder();
            fieldDef.append("\"").append(fieldName).append("\" ").append(duckDbType);
            
            if (schemaInfo.getPrimaryKeys().contains(fieldName)) {
                fieldDef.append(" PRIMARY KEY");
            }
            
            fieldDefs.add(fieldDef.toString());
        }
        
        sql.append(String.join(", ", fieldDefs));
        sql.append(")");
        
        executeUpdate(sql.toString());
        logger.debug("Created table: {} from NodeSchemaInfo", tableName);
    }

    /**
     * 将Tap类型转换为DuckDB类型
     */
    private String convertTapTypeToDuckDbType(TapField tapField) {
        // 优先使用 dataType
        String dataType = tapField.getDataType();
        if (dataType != null && !dataType.isEmpty()) {
            return dataType.toUpperCase();
        }
        
        // 其次使用 tapType
        TapType tapType = tapField.getTapType();
        if (tapType != null) {
            String typeName = tapType.getClass().getSimpleName();
            return switch (typeName) {
                case "TapNumber" -> "BIGINT";
                case "TapBoolean" -> "BOOLEAN";
                case "TapDate", "TapDateTime" -> "TIMESTAMP";
                case "TapString" -> "VARCHAR";
                case "TapBinary" -> "BLOB";
                default -> "VARCHAR";
            };
        }
        
        return "VARCHAR";
    }

    // ==================== DDL 操作封装 ====================

    @Override
    public void dropTable(String tableName) throws SQLException {
        checkClosed();
        executeUpdate("DROP TABLE IF EXISTS " + tableName);
        logger.debug("Dropped table: {}", tableName);
    }

    @Override
    public void truncateTable(String tableName) throws SQLException {
        checkClosed();
        executeUpdate("DELETE FROM " + tableName);
        logger.debug("Truncated table: {}", tableName);
    }

    @Override
    public void renameTable(String oldTableName, String newTableName) throws SQLException {
        checkClosed();
        executeUpdate("ALTER TABLE " + oldTableName + " RENAME TO " + newTableName);
        logger.debug("Renamed table: {} -> {}", oldTableName, newTableName);
    }

    @Override
    public void addColumn(String tableName, String columnName, String columnType) throws SQLException {
        checkClosed();
        executeUpdate("ALTER TABLE " + tableName + " ADD COLUMN \"" + columnName + "\" " + columnType);
        logger.debug("Added column: {}.{} {}", tableName, columnName, columnType);
    }

    @Override
    public void dropColumn(String tableName, String columnName) throws SQLException {
        checkClosed();
        executeUpdate("ALTER TABLE " + tableName + " DROP COLUMN \"" + columnName + "\"");
        logger.debug("Dropped column: {}.{}", tableName, columnName);
    }

    @Override
    public void createIndex(String tableName, String indexName, List<String> columnNames, boolean isUnique) throws SQLException {
        checkClosed();
        String uniqueClause = isUnique ? "UNIQUE " : "";
        String columns = columnNames.stream()
                .map(c -> "\"" + c + "\"")
                .collect(java.util.stream.Collectors.joining(", "));
        executeUpdate("CREATE " + uniqueClause + "INDEX IF NOT EXISTS " + indexName + " ON " + tableName + " (" + columns + ")");
        logger.debug("Created {}index: {} on table {}({})", isUnique ? "unique " : "", indexName, tableName, String.join(",", columnNames));
    }

    @Override
    public void dropIndex(String tableName, String indexName) throws SQLException {
        checkClosed();
        executeUpdate("DROP INDEX IF EXISTS " + indexName);
        logger.debug("Dropped index: {} on table {}", indexName, tableName);
    }

    // ==================== DML 操作封装 ====================

    @Override
    public void insert(String tableName, Map<String, Object> data) throws SQLException {
        checkClosed();
        
        if (data.isEmpty()) {
            return;
        }
        
        List<String> columns = new ArrayList<>(data.keySet());
        List<Object> values = new ArrayList<>();
        for (String col : columns) {
            values.add(data.get(col));
        }
        
        String sql = buildInsertSql(tableName, columns, values);
        executeUpdate(sql);
        logger.debug("Inserted 1 row into table: {}", tableName);
    }

    @Override
    public void insertBatch(String tableName, List<TapdataEvent> dataList) throws SQLException, java.io.IOException {
        checkClosed();
        
        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        
        List<Map<String, Object>> rows = new ArrayList<>(dataList.size());
        for (TapdataEvent event : dataList) {
            if (event == null) {
                continue;
            }
            TapEvent tapEvent = event.getTapEvent();
            if (tapEvent instanceof TapRecordEvent recordEvent) {
                Map<String, Object> after = TapEventUtil.getAfter(recordEvent);
                if (after != null && !after.isEmpty()) {
                    rows.add(after);
                }
            }
        }

        if (rows.isEmpty()) {
            return;
        }

        writeBatch(rows, tableName);
        logger.debug("Inserted {} rows into table: {}", rows.size(), tableName);
    }

    @Override
    public void insertBatch(NodeSchemaInfo schemaInfo, List<TapdataEvent> dataList) throws SQLException, java.io.IOException {
        checkClosed();
        
        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        
        String targetTableName = schemaInfo.getTargetTableName();
        
        List<Map<String, Object>> batchData = new ArrayList<>();
        for (TapdataEvent event : dataList) {
            if (event == null) {
                continue;
            }
            TapEvent tapEvent = event.getTapEvent();
            if (tapEvent instanceof TapRecordEvent recordEvent) {
                Map<String, Object> after = TapEventUtil.getAfter(recordEvent);
                if (after != null && !after.isEmpty()) {
                    batchData.add(after);
                }
            }
        }
        
        if (batchData.isEmpty()) {
            return;
        }
        
        writeBatch(batchData, schemaInfo);
    }

    @Override
    public int update(String tableName, Map<String, Object> data, String whereClause) throws SQLException {
        checkClosed();
        
        if (data.isEmpty()) {
            return 0;
        }
        
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ").append(tableName).append(" SET ");
        
        List<String> setClauses = new ArrayList<>();
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            setClauses.add("\"" + entry.getKey() + "\" = " + formatValue(entry.getValue()));
        }
        sql.append(String.join(", ", setClauses));
        
        if (whereClause != null && !whereClause.isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }
        
        int affectedRows = executeUpdate(sql.toString());
        logger.debug("Updated {} rows in table: {}", affectedRows, tableName);
        return affectedRows;
    }

    @Override
    public int delete(String tableName, String whereClause) throws SQLException {
        checkClosed();
        
        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(tableName);
        
        if (whereClause != null && !whereClause.isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }
        
        int affectedRows = executeUpdate(sql.toString());
        logger.debug("Deleted {} rows from table: {}", affectedRows, tableName);
        return affectedRows;
    }

    @Override
    public void upsert(String tableName, Map<String, Object> data) throws SQLException {
        checkClosed();
        
        if (data.isEmpty()) {
            return;
        }
        
        List<String> columns = new ArrayList<>(data.keySet());
        List<Object> values = new ArrayList<>();
        for (String col : columns) {
            values.add(data.get(col));
        }
        
        String sql = buildUpsertSql(tableName, columns, values);
        executeUpdate(sql);
        logger.debug("Upserted 1 row into table: {}", tableName);
    }

    @Override
    public void upsertBatch(String tableName, List<Map<String, Object>> dataList) throws SQLException, java.io.IOException {
        checkClosed();
        
        if (dataList.isEmpty()) {
            return;
        }
        
        // 使用Arrow批量写入（INSERT OR REPLACE）
        writeBatch(dataList, tableName);
        logger.debug("Upserted {} rows into table: {}", dataList.size(), tableName);
    }

    // ==================== 事务管理 ====================

    @Override
    public void commit() throws SQLException {
        checkClosed();
        connection.commit();
        logger.debug("Transaction committed");
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        connection.rollback();
        logger.debug("Transaction rolled back");
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        connection.setAutoCommit(autoCommit);
        logger.debug("Auto-commit set to: {}", autoCommit);
    }

    // ==================== 元数据操作 ====================

    @Override
    public boolean tableExists(String tableName) throws SQLException {
        checkClosed();
        
        String sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, tableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        }
        return false;
    }

    @Override
    public List<Map<String, Object>> getTableColumns(String tableName) throws SQLException {
        checkClosed();
        
        List<Map<String, Object>> columns = new ArrayList<>();
        
        String sql = "SELECT column_name, data_type, is_nullable, column_default " +
                     "FROM information_schema.columns WHERE table_name = ?";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, tableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                
                while (rs.next()) {
                    Map<String, Object> columnInfo = new LinkedHashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        columnInfo.put(metaData.getColumnName(i), rs.getObject(i));
                    }
                    columns.add(columnInfo);
                }
            }
        }
        
        return columns;
    }

    @Override
    public List<String> listTables() throws SQLException {
        checkClosed();
        
        List<String> tables = new ArrayList<>();
        
        String sql = "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'";
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                tables.add(rs.getString("table_name"));
            }
        }
        
        return tables;
    }

    @Override
    public long getRowCount(String tableName) throws SQLException {
        checkClosed();
        
        String sql = "SELECT COUNT(*) FROM " + tableName;
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        
        return 0;
    }

    @Override
    public boolean isConnectionValid() {
        if (closed.get() || connection == null) {
            return false;
        }
        
        try {
            return connection.isValid(5);
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * 构建INSERT SQL
     */
    private String buildInsertSql(String tableName, List<String> columns, List<Object> values) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(tableName);
        sb.append(" (").append(String.join(", ", columns.stream().map(c -> "\"" + c + "\"").toList())).append(") VALUES (");
        
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(formatValue(values.get(i)));
        }
        
        sb.append(")");
        return sb.toString();
    }

    /**
     * 构建UPSERT SQL（INSERT OR REPLACE）
     */
    private String buildUpsertSql(String tableName, List<String> columns, List<Object> values) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT OR REPLACE INTO ").append(tableName);
        sb.append(" (").append(String.join(", ", columns.stream().map(c -> "\"" + c + "\"").toList())).append(") VALUES (");
        
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(formatValue(values.get(i)));
        }
        
        sb.append(")");
        return sb.toString();
    }

    /**
     * 格式化SQL值
     */
    private String formatValue(Object value) {
        if (value == null) {
            return "NULL";
        } else if (value instanceof String) {
            return "'" + escapeString((String) value) + "'";
        } else if (value instanceof java.util.Date) {
            // 正确格式化日期时间
            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            return "'" + sdf.format((java.util.Date) value) + "'";
        } else if (value instanceof java.time.LocalDateTime) {
            java.time.format.DateTimeFormatter fmt = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
            return "'" + ((java.time.LocalDateTime) value).format(fmt) + "'";
        } else if (value instanceof java.time.LocalDate) {
            return "'" + value.toString() + "'";
        } else if (value instanceof java.time.LocalTime) {
            return "'" + value.toString() + "'";
        } else if (value instanceof Boolean) {
            return value.toString().toUpperCase();
        } else {
            return value.toString();
        }
    }

    /**
     * 转义字符串中的特殊字符
     */
    private String escapeString(String str) {
        if (str == null) {
            return "";
        }
        return str.replace("'", "''");
    }

    /**
     * 检查是否已关闭
     */
    private void checkClosed() throws SQLException {
        if (closed.get()) {
            throw new SQLException("DuckDbOperator has been closed");
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            logger.info("Closing DuckDbOperator");
            
            // 刷新剩余数据（只有当缓冲区有数据时）
            if (!batchBuffer.isEmpty()) {
                try {
                    // 推断TapTable结构
                    TapTable tapTable = inferTapTable(new ArrayList<>(batchBuffer), "temp_batch_table");
                    // 只有当有字段定义时才刷新
                    if (tapTable.getNameFieldMap() != null && !tapTable.getNameFieldMap().isEmpty()) {
                        flushBatch("temp_batch_table", tapTable);
                    } else {
                        logger.debug("Skipping batch flush - no table schema available");
                        batchBuffer.clear();
                    }
                } catch (SQLException | java.io.IOException e) {
                    logger.warn("Failed to flush batch during close: {}", e.getMessage());
                }
            }
            
            // 关闭ArrowWriter
            if (arrowWriter != null) {
                try {
                    arrowWriter.close();
                } catch (Exception e) {
                    logger.warn("Failed to close ArrowWriter: {}", e.getMessage());
                }
            }
            
            // 关闭连接
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.warn("Failed to close DuckDB connection: {}", e.getMessage());
                }
            }
            
            logger.info("DuckDbOperator closed successfully");
        }
    }

    /**
     * 获取ArrowWriter（用于测试或特殊场景）
     */
    ArrowWriter getArrowWriter() {
        return arrowWriter;
    }

    /**
     * 检查是否有待刷新的批数据
     */
    boolean hasPendingBatch() {
        return !batchBuffer.isEmpty();
    }

    /**
     * 获取批缓冲区大小
     */
    int getBatchBufferSize() {
        return batchBuffer.size();
    }

    /**
     * 获取数据库文件路径
     * @return 文件路径（null表示内存模式）
     */
    public String getDbPath() {
        return dbPath;
    }

    /**
     * 检查是否为持久化文件模式
     * @return true=文件模式，false=内存模式
     */
    public boolean isPersistentMode() {
        return dbPath != null && !dbPath.trim().isEmpty();
    }

    // ==================== 新增: 物化视图相关方法的实现 ====================

    @Override
    public java.util.Map<Object, java.util.Map<String, Object>> queryForMap(String sql, String primaryKeyField) throws SQLException {
        checkClosed();
        java.util.List<java.util.Map<String, Object>> results = executeQuery(sql);
        java.util.Map<Object, java.util.Map<String, Object>> map = new java.util.LinkedHashMap<>();
        for (java.util.Map<String, Object> row : results) {
            Object pk = row.get(primaryKeyField);
            if (pk != null) {
                map.put(pk, row);
            }
        }
        return map;
    }

    @Override
    public void executeInTransaction(ThrowingConsumer action) throws SQLException, java.io.IOException {
        checkClosed();
        boolean originalAutoCommit = connection.getAutoCommit();
        try {
            connection.setAutoCommit(false);
            action.accept();
            connection.commit();
        } catch (SQLException | java.io.IOException e) {
            connection.rollback();
            throw e;
        } catch (RuntimeException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(originalAutoCommit);
        }
    }

    @Override
    public int batchInsert(String tableName, java.util.List<java.util.Map<String, Object>> dataList) throws SQLException, java.io.IOException {
        checkClosed();
        if (dataList == null || dataList.isEmpty()) {
            return 0;
        }
        writeBatch(dataList, tableName);
        return dataList.size();
    }

    // ==================== Table Lifecycle Management Implementation ====================

    @Override
    public void ensureTableExists(String tableName, List<TapField> fields, 
                                 List<String> primaryKeys, boolean recreate) throws SQLException {
        Objects.requireNonNull(tableName, "tableName must not be null");
        Objects.requireNonNull(fields, "fields must not be null");
        Objects.requireNonNull(primaryKeys, "primaryKeys must not be null");
        
        String safeTableName = sanitizeIdentifier(tableName);
        
        if (recreate) {
            logger.info("Recreating table: {}", safeTableName);
            
            String dropSql = "DROP TABLE IF EXISTS " + safeTableName;
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(dropSql);
            }
            
            String createSql = buildCreateTableSql(safeTableName, fields, primaryKeys);
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(createSql);
            }
            
            logger.debug("Successfully recreated table: {}", safeTableName);
        } else {
            if (tableExists(safeTableName)) {
                logger.debug("Table already exists: {}, skipping creation", safeTableName);
                return;
            }
            
            logger.info("Creating new table: {}", safeTableName);
            
            String createSql = buildCreateTableSql(safeTableName, fields, primaryKeys);
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(createSql);
            }
            
            logger.debug("Successfully created table: {}", safeTableName);
        }
    }

    public static String buildCreateTableSql(String tableName, List<TapField> fields, 
                                            List<String> primaryKeys) {
        Objects.requireNonNull(tableName, "tableName must not be null");
        Objects.requireNonNull(fields, "fields must not be null");
        
        String safeTableName = sanitizeIdentifier(tableName);
        
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(safeTableName).append(" (\n");
        
        List<String> columnDefs = new ArrayList<>();
        for (TapField field : fields) {
            if (field == null || field.getName() == null) {
                continue;
            }
            
            String colName = sanitizeIdentifier(field.getName());
            String colType = mapToDuckDbType(field.getTapType());
            
            StringBuilder def = new StringBuilder()
                .append("  ").append(colName).append(" ").append(colType);
            
            if (primaryKeys != null && primaryKeys.contains(field.getName())) {
                def.append(" PRIMARY KEY NOT NULL");
            }
            
            columnDefs.add(def.toString());
        }
        
        if (columnDefs.isEmpty()) {
            throw new IllegalArgumentException(
                "Cannot create table '" + safeTableName + "' with no valid columns");
        }
        
        sql.append(String.join(",\n", columnDefs));
        sql.append("\n)");
        
        return sql.toString();
    }

    public static String sanitizeIdentifier(String identifier) {
        if (identifier == null || identifier.isBlank()) {
            throw new IllegalArgumentException(
                "Cannot sanitize null or blank identifier");
        }
        
        String sanitized = identifier.replaceAll("[^A-Za-z0-9_]", "_");
        
        if (Character.isDigit(sanitized.charAt(0))) {
            sanitized = "_" + sanitized;
        }
        
        return sanitized;
    }

    /**
     * Map TapType to DuckDB data type string
     * @param tapType Tapdata type
     * @return DuckDB type string (e.g., "BIGINT", "VARCHAR", "TIMESTAMP")
     */
    static String mapToDuckDbType(TapType tapType) {
        if (tapType == null) {
            return "VARCHAR";
        }
        
        String className = tapType.getClass().getSimpleName();
        
        switch (className) {
            case "TapString":
            case "TapBytes":
                return "VARCHAR";
            case "TapNumber":
                return "BIGINT";
            case "TapDate":
            case "TapDateTime":
                return "TIMESTAMP";
            case "TapTime":
                return "TIME";
            case "TapBoolean":
                return "BOOLEAN";
            case "TapBinary":
                return "BLOB";
            default:
                return "VARCHAR";
        }
    }
}