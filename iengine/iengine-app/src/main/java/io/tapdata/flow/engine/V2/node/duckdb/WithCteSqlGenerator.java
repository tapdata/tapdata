package io.tapdata.flow.engine.V2.node.duckdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * WITH CTE SQL 生成器
 * 将 CDC 数据嵌入 WITH 子句，生成完整的 SQL 语句
 * 
 * 生成格式：
 * WITH table_name(field1, field2) AS (VALUES (v1, v2), (v3, v4))
 * SELECT ... FROM table_name ...
 */
public class WithCteSqlGenerator {

    private static final Logger logger = LoggerFactory.getLogger(WithCteSqlGenerator.class);

    /**
     * 构建单行 VALUES 子句
     * @param rowData 行数据
     * @param fields 字段列表（按顺序）
     * @return VALUES (v1, v2, ...) 格式字符串
     */
    public String buildValuesClause(Map<String, Object> rowData, List<String> fields) {
        List<String> values = new ArrayList<>();
        for (String field : fields) {
            values.add(formatValue(rowData.get(field)));
        }
        return "VALUES (" + String.join(", ", values) + ")";
    }

    /**
     * 生成单条 WITH CTE SQL
     * @param sqlTemplate SQL 模板（用户原始 SQL）
     * @param tableName 源表名
     * @param rowData 行数据
     * @param fields 字段列表
     * @return 完整的 WITH CTE SQL
     */
    public String generateSingle(String sqlTemplate, String tableName,
                                 Map<String, Object> rowData, List<String> fields) {
        String valuesClause = buildValuesClause(rowData, fields);
        String sql = String.format("WITH %s AS (%s) AS t(%s) %s",
                tableName, valuesClause, String.join(", ", fields), sqlTemplate);
        logger.debug("Generated single WITH CTE SQL for table {}: {}", tableName, sql);
        return sql;
    }

    /**
     * 生成批量 WITH CTE SQL
     * @param sqlTemplate SQL 模板
     * @param tableName 源表名
     * @param rows 多行数据
     * @param fields 字段列表
     * @return 完整的批量 WITH CTE SQL
     */
    public String generateBatch(String sqlTemplate, String tableName,
                                List<Map<String, Object>> rows, List<String> fields) {
        if (rows == null || rows.isEmpty()) {
            throw new IllegalArgumentException("rows cannot be null or empty");
        }

        List<String> valueRows = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            String valuesClause = buildValuesClause(row, fields);
            // Strip "VALUES " prefix, keep just the parenthesized values
            valueRows.add(valuesClause.substring("VALUES ".length()));
        }
        String valuesClause = "VALUES " + String.join(", ", valueRows);
        String sql = String.format("WITH %s (%s) AS (%s) %s",
                tableName, String.join(", ", fields), valuesClause, sqlTemplate);
        logger.debug("Generated batch WITH CTE SQL for table {} ({} rows)", tableName, rows.size());
        return sql;
    }

    /**
     * 格式化值（处理字符串转义、NULL、数字、布尔等）
     * 
     * @deprecated 使用 {@link DuckDbSqlValueFormatter#format(Object)} 替代
     */
    @Deprecated
    protected String formatValue(Object value) {
        return DuckDbSqlValueFormatter.format(value);
    }
}
