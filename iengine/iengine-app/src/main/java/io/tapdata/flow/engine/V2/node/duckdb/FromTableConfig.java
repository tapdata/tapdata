package io.tapdata.flow.engine.V2.node.duckdb;

import java.util.List;

/**
 * Configuration for a source (from) table in the materialized view.
 */
public class FromTableConfig {
    private String tableName;
    private String primaryKey;
    private String querySql;
    private List<String> fields;

    public FromTableConfig() {}

    public FromTableConfig(String tableName, String primaryKey) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
    }

    public FromTableConfig(String tableName, String primaryKey, String querySql, List<String> fields) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.querySql = querySql;
        this.fields = fields;
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

    public String getQuerySql() {
        return querySql;
    }

    public void setQuerySql(String querySql) {
        this.querySql = querySql;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }
}
