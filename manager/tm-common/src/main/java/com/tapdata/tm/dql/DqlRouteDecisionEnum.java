package com.tapdata.tm.dql;

public enum DqlRouteDecisionEnum {
    RECORD_DLQ,
    TASK_RETRY,
    TASK_ERROR;

    public static DqlRouteDecisionEnum parse(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        for (DqlRouteDecisionEnum decision : values()) {
            if (decision.name().equalsIgnoreCase(value)) {
                return decision;
            }
        }
        return null;
    }
}
