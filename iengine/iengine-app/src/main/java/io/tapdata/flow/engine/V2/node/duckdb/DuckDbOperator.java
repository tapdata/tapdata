package io.tapdata.flow.engine.V2.node.duckdb;

import io.tapdata.entity.schema.TapTable;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

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
     * 根据TapTable Schema创建临时表
     * @param tapTable 表结构定义
     * @param tempTableName 临时表名
     * @throws SQLException SQL执行异常
     */
    void createTempTable(TapTable tapTable, String tempTableName) throws SQLException;

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
}