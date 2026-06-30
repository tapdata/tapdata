package io.tapdata.flow.engine.V2.node.duckdb;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.dag.process.dto.TapTableDto;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * DuckDB 操作接口 - 定义 DuckDB 操作契约
 * 核心功能：
 * 1. SQL 查询执行
 * 2. SQL 更新执行（DML/DDL）
 * 3. Arrow 零拷贝批量写入
 * 4. 表结构管理（CREATE/DROP/TRUNCATE）
 */
public interface DuckDbOperator extends AutoCloseable {

    /**
     * 关闭资源
     * @throws SQLException SQL执行异常
     */
    @Override
    void close() throws SQLException;

    /**
     * 执行查询SQL
     * @param sql 查询语句
     * @return 查询结果列表
     * @throws SQLException SQL执行异常
     */
    List<Map<String, Object>> executeQuery(String sql) throws SQLException;

    /**
     * 按批消费查询结果。
     *
     * <p>默认实现会先调用 {@link #executeQuery(String)} 再拆批，具体实现可覆盖为真正的流式读取，
     * 以避免一次性将全部结果加载到内存中。</p>
     *
     * @param sql 查询语句
     * @param batchSize 每批大小，<=0 时按 1 处理
     * @param batchConsumer 批量消费回调
     * @throws SQLException SQL执行异常
     */
    void executeQueryInBatches(String sql, int batchSize, Predicate<Boolean> isAlive, Consumer<Map<String, Object>> batchConsumer, Consumer<Boolean> finalCall) throws SQLException;

    /**
     * 执行更新SQL（INSERT/UPDATE/DELETE/DDL）
     * @param sql DML或DDL语句
     * @return 影响行数
     * @throws SQLException SQL执行异常
     */
    int executeUpdate(String sql) throws SQLException;

    /**
     * 执行任意SQL，判断是否有结果集
     * @param sql SQL语句
     * @return 执行结果对象，包含是否有结果集、结果列表、更新计数
     * @throws SQLException SQL执行异常
     */
    ExecuteResult execute(String sql) throws SQLException;

    /**
     * SQL执行结果对象
     */
    class ExecuteResult {
        private final boolean hasResultSet;
        private final List<Map<String, Object>> resultSet;
        private final int updateCount;

        public ExecuteResult(boolean hasResultSet, List<Map<String, Object>> resultSet, int updateCount) {
            this.hasResultSet = hasResultSet;
            this.resultSet = resultSet;
            this.updateCount = updateCount;
        }

        public boolean isHasResultSet() {
            return hasResultSet;
        }

        public List<Map<String, Object>> getResultSet() {
            return resultSet;
        }

        public int getUpdateCount() {
            return updateCount;
        }
    }

    /**
     * 使用Arrow零拷贝批量写入数据
     * @param data 数据列表
     * @param tableName 目标表名
     * @throws SQLException SQL执行异常
     */
    void writeBatch(List<Map<String, Object>> data, String tableName) throws SQLException, java.io.IOException;

    /**
     * 使用Arrow零拷贝批量写入数据（使用TapTableDto，优先使用预计算类型）
     * @param data 数据列表
     * @param tableName 目标表名
     * @param tapTableDto 表结构DTO，包含预计算类型信息
     * @throws SQLException SQL执行异常
     */
    void writeBatch(List<Map<String, Object>> data, String tableName, TapTableDto tapTableDto) throws SQLException, java.io.IOException;

    /**
     * 使用Arrow零拷贝批量写入数据（使用NodeSchemaInfo，从缓存获取预计算Schema）
     * @param data 数据列表
     * @param schemaInfo 预加载的Schema信息
     * @throws SQLException SQL执行异常
     */
    void writeBatch(List<Map<String, Object>> data, NodeSchemaInfo schemaInfo) throws SQLException, java.io.IOException;

    boolean isBatchWritingEnabled();

    void setBatchWritingEnabled(boolean batchWritingEnabled);
    /**
     * 根据TapTable Schema创建表
     * @param tapTable 表结构定义
     * @throws SQLException SQL执行异常
     */
    void createTable(TapTable tapTable) throws SQLException;
    
    /**
     * 根据TapTableDto Schema创建表（优先使用预计算类型）
     * @param tapTableDto 表结构DTO，包含预计算类型信息
     * @throws SQLException SQL执行异常
     */
    void createTable(TapTableDto tapTableDto) throws SQLException;

    /**
     * 根据NodeSchemaInfo创建表（从缓存获取预计算Schema）
     * @param schemaInfo 预加载的Schema信息
     * @throws SQLException SQL执行异常
     */
    void createTable(NodeSchemaInfo schemaInfo) throws SQLException;

    /**
     * 根据TapTable Schema创建表
     * @param tapTable 表结构定义
     * @param useTempTable 是否使用临时表
     * @throws SQLException SQL执行异常
     */
    void createTable(TapTable tapTable, boolean useTempTable) throws SQLException;

    /**
     * 根据TapTable Schema创建临时表
     * @param tapTable 表结构定义
     * @param tempTableName 临时表名
     * @throws SQLException SQL执行异常
     */
    void createTempTable(TapTable tapTable, String tempTableName) throws SQLException;

    /**
     * 根据TapTable Schema创建临时表
     * @param tapTable 表结构定义
     * @param tempTableName 临时表名
     * @param useTempTable 是否使用临时表
     * @throws SQLException SQL执行异常
     */
    void createTempTable(TapTable tapTable, String tempTableName, boolean useTempTable) throws SQLException;

    /**
     * 根据NodeSchemaInfo创建临时表
     * @param schemaInfo 预加载的Schema信息
     * @throws SQLException SQL执行异常
     */
    void createTempTable(NodeSchemaInfo schemaInfo) throws SQLException;

    /**
     * 根据NodeSchemaInfo创建临时表
     * @param schemaInfo 预加载的Schema信息
     * @param useTempTable 是否使用临时表
     * @throws SQLException SQL执行异常
     */
    void createTempTable(NodeSchemaInfo schemaInfo, boolean useTempTable) throws SQLException;

    // ==================== DDL 操作封装 ====================

    /**
     * 删除表
     * @param tableName 表名
     * @throws SQLException SQL执行异常
     */
    void dropTable(String tableName) throws SQLException;

    /**
     * 清空表数据
     * @param tableName 表名
     * @throws SQLException SQL执行异常
     */
    void truncateTable(String tableName) throws SQLException;

    /**
     * 重命名表
     * @param oldTableName 旧表名
     * @param newTableName 新表名
     * @throws SQLException SQL执行异常
     */
    void renameTable(String oldTableName, String newTableName) throws SQLException;

    /**
     * 添加列
     * @param tableName 表名
     * @param columnName 列名
     * @param columnType 列类型
     * @throws SQLException SQL执行异常
     */
    void addColumn(String tableName, String columnName, String columnType) throws SQLException;

    /**
     * 删除列
     * @param tableName 表名
     * @param columnName 列名
     * @throws SQLException SQL执行异常
     */
    void dropColumn(String tableName, String columnName) throws SQLException;

    /**
     * 创建索引
     * @param tableName 表名
     * @param indexName 索引名
     * @param columnNames 列名列表
     * @param isUnique 是否唯一索引
     * @throws SQLException SQL执行异常
     */
    void createIndex(String tableName, String indexName, List<String> columnNames, boolean isUnique) throws SQLException;

    /**
     * 删除索引
     * @param tableName 表名
     * @param indexName 索引名
     * @throws SQLException SQL执行异常
     */
    void dropIndex(String tableName, String indexName) throws SQLException;

    // ==================== Table Lifecycle Management ====================

    /**
     * Ensure table exists with correct schema, optionally recreate
     * 
     * <p>This method provides unified table lifecycle management. When recreate is true,
     * it drops the existing table and creates a new one. When false, it only creates
     * the table if it doesn't already exist.</p>
     * 
     * @param tableName Target table name (will be sanitized)
     * @param fields Field definitions from NodeSchemaInfo/TapTable
     * @param primaryKeys Primary key field names
     * @param recreate If true, drop and recreate; if false, create only if not exists
     * @throws SQLException if database operation fails
     */
    void ensureTableExists(String tableName, List<TapField> fields, 
                          List<String> primaryKeys, boolean recreate) throws SQLException;

    /**
     * 确保表存在，使用 NodeSchemaInfo 中的有序字段
     * 
     * @param schemaInfo 包含完整表结构信息的 NodeSchemaInfo
     * @param recreate 是否删除并重建表
     * @throws SQLException 如果数据库操作失败
     */
    void ensureTableExists(NodeSchemaInfo schemaInfo, boolean recreate) throws SQLException;

    /**
     * Build CREATE TABLE SQL statement from TapField definitions
     * 
     * <p>This is a static utility method that generates valid DuckDB CREATE TABLE syntax.
     * All identifiers are sanitized to prevent SQL injection.</p>
     * 
     * @param tableName Target table name (will be sanitized)
     * @param fields Field definitions
     * @param primaryKeys Primary key field names
     * @return Complete CREATE TABLE SQL statement
     */
    static String buildCreateTableSql(String tableName, List<TapField> fields, 
                                      List<String> primaryKeys) {
        return DuckDbOperatorImpl.buildCreateTableSql(tableName, fields, primaryKeys);
    }

    /**
     * Sanitize identifier for safe use in SQL
     * @param identifier Raw identifier
     * @return Sanitized identifier (only alphanumeric and underscores)
     */
    static String sanitizeIdentifier(String identifier) {
        return DuckDbOperatorImpl.sanitizeIdentifier(identifier);
    }

    // ==================== DML 操作封装 ====================

    /**
     * 插入单行数据
     * @param tableName 表名
     * @param data 数据映射
     * @throws SQLException SQL执行异常
     */
    void insert(String tableName, Map<String, Object> data) throws SQLException;

    /**
     * 批量插入数据（初始化阶段使用 TapdataEvent 直接写入）
     * @param tableName 表名
     * @param dataList TapdataEvent 列表
     * @throws SQLException SQL执行异常
     */
    void insertBatch(String tableName, List<TapdataEvent> dataList) throws SQLException, java.io.IOException;

    /**
     * 批量插入数据（初始化阶段使用 TapdataEvent 直接写入，使用预计算Schema）
     * @param schemaInfo 预加载的Schema信息
     * @param dataList TapdataEvent 列表
     * @throws SQLException SQL执行异常
     */
    void insertBatch(NodeSchemaInfo schemaInfo, List<TapdataEvent> dataList) throws SQLException, java.io.IOException;

    /**
     * 更新数据
     * @param tableName 表名
     * @param data 更新数据
     * @param whereClause WHERE条件
     * @throws SQLException SQL执行异常
     */
    int update(String tableName, Map<String, Object> data, String whereClause) throws SQLException;

    /**
     * 删除数据
     * @param tableName 表名
     * @param whereClause WHERE条件
     * @return 影响行数
     * @throws SQLException SQL执行异常
     */
    int delete(String tableName, String whereClause) throws SQLException;

    /**
     * 按主键批量删除行（支持单主键和复合主键，类型安全）
     *
     * <p>此方法从 NodeSchemaInfo 获取主键列名和类型，自动进行精准的类型转换，
     * 彻底避免 VARCHAR 与 BIGINT 比较错误。</p>
     *
     * <h3>单主键用法：</h3>
     * <pre>{@code
     * List<Object> pkValues = Arrays.asList(1L, 2L, 3L);
     * operator.deleteByIds(pkValues, schemaInfo);
     * }</pre>
     *
     * <h3>复合主键用法：</h3>
     * <pre>{@code
     * List<Object> pkValues = new ArrayList<>();
     * Map<String, Object> pk1 = new LinkedHashMap<>();
     * pk1.put("id", 1);
     * pk1.put("type", "admin");
     * pkValues.add(pk1);
     * operator.deleteByIds(pkValues, schemaInfo);
     * }</pre>
     *
     * <h3>SQL 生成规则：</h3>
     * <ul>
     *   <li>单主键：DELETE FROM tbl WHERE pk IN (SELECT pk FROM (VALUES (v1),(v2)) AS t(pk))</li>
     *   <li>复合主键：DELETE FROM tbl WHERE (pk1,pk2) IN (VALUES (v1,v2), (v3,v4))</li>
     * </ul>
     *
     * <h3>类型转换规则：</h3>
     * <ul>
     *   <li>Number → 直接输出（无引号）</li>
     *   <li>String + 目标列是 TapNumber → 尝试解析为数值，成功则无引号输出</li>
     *   <li>String + 目标列是 TapString → 加单引号并转义</li>
     *   <li>Boolean → TRUE / FALSE</li>
     *   <li>时间类型 → 标准 SQL 时间格式字符串（加引号）</li>
     * </ul>
     *
     * @param pkValues 主键值列表。单主键时为值列表；复合主键时为 Map 列表（key=列名）
     * @param schemaInfo 包含完整表结构信息的 NodeSchemaInfo
     * @return 删除的行数
     * @throws IllegalArgumentException 如果 pkValues 为空或 schemaInfo 未定义主键
     * @throws SQLException 如果数据库操作失败
     */
    int deleteByIds(List<Object> pkValues, NodeSchemaInfo schemaInfo) throws SQLException;

    /**
     * UPSERT操作（INSERT OR REPLACE）
     * @param tableName 表名
     * @param data 数据
     * @throws SQLException SQL执行异常
     */
    void upsert(String tableName, Map<String, Object> data) throws SQLException;

    /**
     * 批量UPSERT操作
     * @param tableName 表名
     * @param dataList 数据列表
     * @throws SQLException SQL执行异常
     */
    void upsertBatch(String tableName, List<Map<String, Object>> dataList) throws SQLException, java.io.IOException;

    // ==================== 事务管理 ====================

    /**
     * 提交事务
     * @throws SQLException SQL执行异常
     */
    void commit() throws SQLException;

    /**
     * 回滚事务
     * @throws SQLException SQL执行异常
     */
    void rollback() throws SQLException;

    /**
     * 设置自动提交
     * @param autoCommit 是否自动提交
     * @throws SQLException SQL执行异常
     */
    void setAutoCommit(boolean autoCommit) throws SQLException;

    // ==================== 元数据操作 ====================

    /**
     * 检查表是否存在
     * @param tableName 表名
     * @return true表示存在
     * @throws SQLException SQL执行异常
     */
    boolean tableExists(String tableName) throws SQLException;

    /**
     * 获取表的列信息
     * @param tableName 表名
     * @return 列信息列表
     * @throws SQLException SQL执行异常
     */
    List<Map<String, Object>> getTableColumns(String tableName) throws SQLException;

    /**
     * 获取数据库中的所有表名
     * @return 表名列表
     * @throws SQLException SQL执行异常
     */
    List<String> listTables() throws SQLException;

    /**
     * 获取表的行数
     * @param tableName 表名
     * @return 行数
     * @throws SQLException SQL执行异常
     */
    long getRowCount(String tableName) throws SQLException;

    /**
     * 检查连接是否有效
     * @return true表示连接有效
     */
    boolean isConnectionValid();

    // ==================== 批处理控制 ====================

    /**
     * 刷新批处理缓冲区
     * @param tableName 目标表名
     * @param tapTable 表结构定义
     * @throws SQLException SQL执行异常
     */
    void flushBatch(String tableName, TapTable tapTable) throws SQLException, java.io.IOException;
    
    /**
     * 刷新批处理缓冲区（使用TapTableDto）
     * @param tableName 目标表名
     * @param tapTableDto 表结构DTO
     * @throws SQLException SQL执行异常
     */
    void flushBatch(String tableName, TapTableDto tapTableDto) throws SQLException, java.io.IOException;

    /**
     * 刷新批处理缓冲区（使用NodeSchemaInfo）
     * @param schemaInfo 预加载的Schema信息
     * @throws SQLException SQL执行异常
     */
    void flushBatch(NodeSchemaInfo schemaInfo) throws SQLException, java.io.IOException;

    // ==================== 新增: 用于物化视图的扩展方法 ====================

    /**
     * 执行查询，返回主键 -> 行数据的 Map
     * @param sql 查询语句
     * @param primaryKeyField 主键字段名
     * @return 主键到行数据的映射
     * @throws SQLException SQL执行异常
     */
    java.util.Map<Object, java.util.Map<String, Object>> queryForMap(String sql, String primaryKeyField) throws SQLException;

    /**
     * 在事务中执行操作
     * @param action 事务内的操作
     * @throws SQLException SQL执行异常
     * @throws java.io.IOException IO异常
     */
    void executeInTransaction(ThrowingConsumer action) throws SQLException, java.io.IOException;

    /**
     * 批量插入数据
     * @param tableName 表名
     * @param dataList 数据列表
     * @return 插入行数
     * @throws SQLException SQL执行异常
     */
    int batchInsert(String tableName, java.util.List<java.util.Map<String, Object>> dataList) throws SQLException, java.io.IOException;

    /**
     * 事务操作回调接口
     */
    @FunctionalInterface
    interface ThrowingConsumer {
        void accept() throws SQLException, java.io.IOException;
    }

    // ==================== SQL 验证工具方法 ====================

    /**
     * SELECT 语句的正则表达式模式
     * 支持：SELECT、WITH、EXPLAIN SELECT、EXPLAIN ANALYZE SELECT 等
     * 支持注释和前导空白
     */
    Pattern SELECT_PATTERN = Pattern.compile(
            "^\\s*(?:--[^\\r\\n]*\\s*)*(?:WITH\\s+|EXPLAIN\\s+)?(?:ANALYZE\\s+)?SELECT\\b",
            Pattern.CASE_INSENSITIVE
    );

    /**
     * 验证 SQL 是否为 SELECT 查询语句
     * @param sql 待验证的 SQL
     * @return true 表示是 SELECT 查询
     */
    static boolean isSelectQuery(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return false;
        }
        return SELECT_PATTERN.matcher(sql).find();
    }

    /**
     * 确保 SQL 是 SELECT 查询语句，否则抛出异常
     * @param sql 待验证的 SQL
     * @param context 上下文描述（用于错误信息）
     * @throws IllegalArgumentException 如果不是 SELECT 查询
     */
    static void ensureSelectQuery(String sql, String context) {
        if (!isSelectQuery(sql)) {
            throw new IllegalArgumentException(
                String.format("Invalid SQL for %s: must be a SELECT query. Found: %s",
                    context,
                    sql.trim().length() > 100 ? sql.trim().substring(0, 100) + "..." : sql.trim())
            );
        }
    }
}
