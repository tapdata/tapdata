package io.tapdata.flow.engine.V2.node.duckdb;

/**
 * Configuration for a source (from) table in the materialized view.
 */
public class FromTableConfig {
    private String tableName;
    private String primaryKey;

    public FromTableConfig() {}

    public FromTableConfig(String tableName, String primaryKey) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }
}
