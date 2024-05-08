package com.tapdata.tm.commons.dag.dynamic;

abstract class DynamicTableStage {
    protected String tableName;
    protected String dynamicRule;
    public DynamicTableStage(String tableName, String dynamicRule) {
        this.tableName = tableName;
        this.dynamicRule = dynamicRule;
    }

    abstract DynamicTableResult genericTableName();
}
