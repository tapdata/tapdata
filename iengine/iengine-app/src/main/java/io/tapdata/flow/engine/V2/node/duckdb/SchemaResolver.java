package io.tapdata.flow.engine.V2.node.duckdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Schema 解析器 - 从 CDC 事件或 DuckDB 元数据自动解析源表字段列表
 * 
 * 解析优先级：
 * 1. 从 CDC 事件的 after 数据推断字段
 * 2. 从 CDC 事件的 before 数据推断字段
 * 3. 从 DuckDB information_schema 查询元数据
 */
public class SchemaResolver {

    private static final Logger logger = LoggerFactory.getLogger(SchemaResolver.class);

    private final DuckDbOperator duckDbOperator;

    public SchemaResolver(DuckDbOperator duckDbOperator) {
        this.duckDbOperator = duckDbOperator;
    }

    /**
     * 解析表字段列表
     * @param tableName 表名
     * @param event CDC 事件（用于推断字段）
     * @return 字段名列表
     */
    public List<String> resolveFields(String tableName, Map<String, Object> event) {
        // 1. 优先从 after 数据推断
        Object after = event.get("after");
        if (after instanceof Map) {
            List<String> fields = new ArrayList<>(((Map<String, Object>) after).keySet());
            if (!fields.isEmpty()) {
                logger.debug("Resolved {} fields from after data for table {}", fields.size(), tableName);
                return fields;
            }
        }

        // 2. 回退到 before 数据推断
        Object before = event.get("before");
        if (before instanceof Map) {
            List<String> fields = new ArrayList<>(((Map<String, Object>) before).keySet());
            if (!fields.isEmpty()) {
                logger.debug("Resolved {} fields from before data for table {}", fields.size(), tableName);
                return fields;
            }
        }

        // 3. 从 DuckDB 元数据查询
        logger.debug("Falling back to DuckDB metadata for table {}", tableName);
        return resolveFieldsFromMetadata(tableName);
    }

    /**
     * 从 DuckDB information_schema 查询表字段
     * @param tableName 表名
     * @return 字段名列表
     */
    public List<String> resolveFieldsFromMetadata(String tableName) {
        String sql = String.format(
            "SELECT column_name FROM information_schema.columns WHERE table_name='%s' ORDER BY ordinal_position",
            tableName
        );

        try {
            List<Map<String, Object>> rows = duckDbOperator.executeQuery(sql);
            List<String> fields = new ArrayList<>();
            for (Map<String, Object> row : rows) {
                Object columnName = row.get("column_name");
                if (columnName != null) {
                    fields.add(columnName.toString());
                }
            }

            if (fields.isEmpty()) {
                logger.warn("No columns found in metadata for table {}", tableName);
            } else {
                logger.info("Resolved {} fields from metadata for table {}", fields.size(), tableName);
            }

            return fields;
        } catch (SQLException e) {
            logger.error("Failed to resolve fields from metadata for table {}: {}", tableName, e.getMessage());
            return new ArrayList<>();
        }
    }
}
