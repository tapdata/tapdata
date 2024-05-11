package com.tapdata.tm.commons.dag.dynamic;

public class DynamicTableResult {
    String oldName;
    String dynamicName;
    public static DynamicTableResult of() {
        return new DynamicTableResult();
    }
    public DynamicTableResult withOldTableName(String oldName) {
        this.oldName = oldName;
        return this;
    }
    public DynamicTableResult withDynamicName(String dynamicName) {
        this.dynamicName = dynamicName;
        return this;
    }

    public String getOldName() {
        return oldName;
    }

    public String getDynamicName() {
        return dynamicName;
    }
}
