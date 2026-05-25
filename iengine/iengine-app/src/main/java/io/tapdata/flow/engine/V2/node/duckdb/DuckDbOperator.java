package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.entity.schema.TapTable;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * DuckDB 操作接口 - 定义 DuckDB 操作契约
 * 
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
     * 根据TapTable Schema创建表
     * @param tapTable 表结构定义
     * @throws SQLException SQL执行异常
     */
    void createTable(TapTable tapTable) throws SQLException;

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

    // ==================== DML 操作封装 ====================

    /**
     * 插入单行数据
     * @param tableName 表名
     * @param data 数据映射
     * @throws SQLException SQL执行异常
     */
    void insert(String tableName, Map<String, Object> data) throws SQLException;

    /**
     * 批量插入数据
     * @param tableName 表名
     * @param dataList 数据列表
     * @throws SQLException SQL执行异常
     */
    void insertBatch(String tableName, List<Map<String, Object>> dataList) throws SQLException, java.io.IOException;

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
        "^\\s*(?:--.*\\s*)*\\s*(?:WITH\\s|EXPLAIN\\s+)?(?:ANALYZE\\s+)?SELECT\\s",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
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