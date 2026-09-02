package com.tapdata.tm.dql;

public enum DqlExceptionScopeEnum {
    RECORD,
    TASK_SHARED,
    SYSTEM,
    UNKNOWN;

    public static DqlExceptionScopeEnum parse(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        for (DqlExceptionScopeEnum scope : values()) {
            if (scope.name().equalsIgnoreCase(value)) {
                return scope;
            }
        }
        return null;
    }
}
