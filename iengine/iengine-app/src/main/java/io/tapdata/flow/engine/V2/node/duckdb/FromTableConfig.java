package io.tapdata.flow.engine.V2.node.duckdb;

/**
 * Configuration for a source (from) table in the materialized view.
 * 
 * <p>This class defines the mapping between a predecessor node and its table alias 
 * used in SQL queries. It enables the system to automatically resolve table aliases 
 * to actual target table names in DuckDB.</p>
 * 
 * <p><b>Usage Example:</b></p>
 * <pre>
 * FromTableConfig config = new FromTableConfig("node_mysql_1", "t1");
 * // preNodeId: "node_mysql_1" → used to lookup NodeSchemaInfo from cache
 * // tableNameInSql: "t1" → alias used in querySql like "SELECT t1.id FROM t1"
 * </pre>
 */
public class FromTableConfig {
    
    /** Predecessor node ID - used to find corresponding NodeSchemaInfo */
    private String preNodeId;
    
    /** Table alias as it appears in SQL queries (e.g., "t1", "t2", "users_alias") */
    private String tableNameInSql;

    public FromTableConfig() {}

    /**
     * Construct with required fields
     * @param preNodeId Predecessor node identifier (must not be blank)
     * @param tableNameInSql Table alias used in SQL (must not be blank)
     * @throws IllegalArgumentException if either parameter is null or blank
     */
    public FromTableConfig(String preNodeId, String tableNameInSql) {
        if (preNodeId == null || preNodeId.isBlank()) {
            throw new IllegalArgumentException(
                "preNodeId must not be null or blank. Got: '" + preNodeId + "'");
        }
        if (tableNameInSql == null || tableNameInSql.isBlank()) {
            throw new IllegalArgumentException(
                "tableNameInSql must not be null or blank. Got: '" + tableNameInSql + "'");
        }
        
        this.preNodeId = preNodeId;
        this.tableNameInSql = tableNameInSql;
    }

    public String getPreNodeId() {
        return preNodeId;
    }

    public void setPreNodeId(String preNodeId) {
        if (preNodeId != null && preNodeId.isBlank()) {
            throw new IllegalArgumentException(
                "preNodeId must not be blank. Got: '" + preNodeId + "'");
        }
        this.preNodeId = preNodeId;
    }

    public String getTableNameInSql() {
        return tableNameInSql;
    }

    public void setTableNameInSql(String tableNameInSql) {
        if (tableNameInSql != null && tableNameInSql.isBlank()) {
            throw new IllegalArgumentException(
                "tableNameInSql must not be blank. Got: '" + tableNameInSql + "'");
        }
        this.tableNameInSql = tableNameInSql;
    }

    @Override
    public String toString() {
        return "FromTableConfig{" +
               "preNodeId='" + preNodeId + '\'' +
               ", tableNameInSql='" + tableNameInSql + '\'' +
               '}';
    }
}
