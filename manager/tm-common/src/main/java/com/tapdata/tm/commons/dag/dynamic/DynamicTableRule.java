package com.tapdata.tm.commons.dag.dynamic;

public enum DynamicTableRule {
    DEFAULT("default", DynamicTableNameByDate.class),
    ;
    String name;
    Class<? extends DynamicTableStage> stage;
    DynamicTableRule(String name, Class<? extends DynamicTableStage> stage) {
        this.name = name;
        this.stage = stage;
    }

    public static DynamicTableRule getRule(String ruleName) {
        if (null == ruleName) return DEFAULT;
        for (DynamicTableRule value : values()) {
            if (value.name.equals(ruleName)) return value;
        }
        return DEFAULT;
    }
}
