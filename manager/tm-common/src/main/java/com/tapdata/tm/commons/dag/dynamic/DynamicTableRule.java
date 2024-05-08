package com.tapdata.tm.commons.dag.dynamic;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

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

    public static DynamicTableResult getDynamicTable(String tableName, String ruleName) {
        Class<? extends DynamicTableStage> stage = getRule(ruleName).stage;
        try {
            Constructor<? extends DynamicTableStage> constructor = stage.getConstructor(String.class, String.class);
            DynamicTableStage dynamicTableStage = constructor.newInstance(tableName, ruleName);
            return dynamicTableStage.genericTableName();
        } catch (InstantiationException e) {

        } catch (InvocationTargetException e) {

        } catch (NoSuchMethodException e) {

        } catch (IllegalAccessException e) {

        }
        return null;
    }
}
