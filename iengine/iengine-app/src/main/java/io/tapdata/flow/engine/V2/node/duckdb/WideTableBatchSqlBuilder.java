package io.tapdata.flow.engine.V2.node.duckdb;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 宽表批量 SQL 生成器
 * 
 * 职责：生成批量 DELETE/INSERT SQL，使用 VALUES 临时表 JOIN 模式
 */
public class WideTableBatchSqlBuilder {

    /**
     * 构建批量删除 SQL
     * 
     * 模板：
     * DELETE FROM {tableName} 
     * WHERE {primaryKey} IN (
     *     SELECT pk FROM (VALUES {valuesClause}) AS t(pk)
     * )
     */
    public static String buildDeleteSql(String tableName, String primaryKey, List<Object> primaryKeys) {
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            throw new IllegalArgumentException("primaryKeys cannot be empty");
        }

        String valuesClause = primaryKeys.stream()
                .map(pk -> "(" + WideTableBatchSqlBuilder.formatValue(pk) + ")")
                .collect(Collectors.joining(", "));

        return String.format(
                "DELETE FROM %s WHERE %s IN (SELECT pk FROM (VALUES %s) AS t(pk))",
                tableName,
                primaryKey,
                valuesClause
        );
    }

    /**
     * 构建批量插入 SQL
     * 
     * 模板：
     * INSERT INTO {tableName} ({columns}) VALUES {rowsClause}
     */
    public static String buildInsertSql(String tableName, List<String> columns, List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty()) {
            throw new IllegalArgumentException("rows cannot be empty");
        }
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("columns cannot be empty");
        }

        String columnsClause = String.join(", ", columns);

        String rowsClause = rows.stream()
                .map(row -> buildRowValues(columns, row))
                .collect(Collectors.joining(", "));

        return String.format(
                "INSERT INTO %s (%s) VALUES %s",
                tableName,
                columnsClause,
                rowsClause
        );
    }

    /**
     * 构建单行 VALUES 子句
     */
    private static String buildRowValues(List<String> columns, Map<String, Object> row) {
        String values = columns.stream()
                .map(col -> formatValue(row.get(col)))
                .collect(Collectors.joining(", "));
        return "(" + values + ")";
    }

    /**
     * 格式化 SQL 值
     */
    static String formatValue(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof String) {
            return "'" + ((String) value).replace("'", "''") + "'";
        }
        if (value instanceof Boolean) {
            return (Boolean) value ? "TRUE" : "FALSE";
        }
        return value.toString();
    }
}
