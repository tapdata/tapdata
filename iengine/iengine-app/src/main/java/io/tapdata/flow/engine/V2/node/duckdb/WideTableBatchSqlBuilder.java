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
     * 构建批量删除 SQL（兼容旧签名）
     * 
     * @deprecated 使用 {@link #buildDeleteSql(String, String, List, Class)} 替代
     */
    @Deprecated
    public static String buildDeleteSql(String tableName, String primaryKey, List<Object> primaryKeys) {
        // 默认按 String 类型处理（向后兼容）
        return buildDeleteSql(tableName, primaryKey, primaryKeys, null);
    }

    /**
     * 构建批量删除 SQL
     * 
     * 模板：
     * DELETE FROM {tableName} 
     * WHERE {primaryKey} IN (
     *     SELECT pk FROM (VALUES {valuesClause}) AS t(pk)
     * )
     * 
     * @param pkTargetType PK 字段的 Java 类型（如 Long.class、String.class）
     *                        用于正确格式化 SQL 值（避免 VARCHAR 与 BIGINT 比较错误）
     */
    public static String buildDeleteSql(String tableName, String primaryKey, 
                                     List<Object> primaryKeys, Class<?> pkTargetType) {
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            throw new IllegalArgumentException("primaryKeys cannot be empty");
        }

        // 根据 pkTargetType 判断是否需要加引号
        boolean quoteStrings = (pkTargetType == null || !Number.class.isAssignableFrom(pkTargetType));
        
        String valuesClause = primaryKeys.stream()
                .map(pk -> "(" + formatPkValue(pk, quoteStrings) + ")")
                .collect(Collectors.joining(", "));

        return String.format(
                "DELETE FROM %s WHERE %s IN (SELECT pk FROM (VALUES %s) AS t(pk))",
                tableName,
                primaryKey,
                valuesClause
        );
    }

    /**
     * 根据目标类型格式化 PK 值
     * 
     * @param pkValue PK 值
     * @param quoteStrings 是否给字符串加引号
     * @return 格式化后的 SQL 字面量
     */
    private static String formatPkValue(Object pkValue, boolean quoteStrings) {
        if (pkValue == null) {
            return "NULL";
        }
        
        // 如果值是数值类型，直接输出（不加引号）
        if (pkValue instanceof Number) {
            return pkValue.toString();
        }
        
        // 如果值是字符串类型
        if (pkValue instanceof String) {
            String strValue = (String) pkValue;
            
            // 优先尝试转换为数值格式（避免 BIGINT 与 VARCHAR 比较错误）
            // 即使 quoteStrings=true，如果字符串内容是数值，也应转换为数值格式
            if (!quoteStrings || isNumericString(strValue)) {
                try {
                    return new java.math.BigDecimal(strValue).stripTrailingZeros().toString();
                } catch (NumberFormatException e) {
                    // 转换失败，按字符串处理
                }
            }
            
            // 字符串类型：加引号
            return "'" + strValue.replace("'", "''") + "'";
        }
        
        // 其他情况：使用 DuckDbSqlValueFormatter 格式化
        return DuckDbSqlValueFormatter.format(pkValue);
    }
    
    /**
     * 判断字符串是否表示数值（整数或小数）
     */
    private static boolean isNumericString(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }
        try {
            new java.math.BigDecimal(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
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
     * 
     * @deprecated 使用 {@link DuckDbSqlValueFormatter#format(Object)} 替代
     */
    @Deprecated
    static String formatValue(Object value) {
        return DuckDbSqlValueFormatter.format(value);
    }
}
