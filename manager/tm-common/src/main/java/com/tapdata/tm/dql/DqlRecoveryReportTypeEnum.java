package com.tapdata.tm.dql;

public enum DqlRecoveryReportTypeEnum {
    BATCH_STARTED,
    BATCH_HEARTBEAT,
    EVENT_STARTED,
    EVENT_RESULT,
    BATCH_FINISHED,
    BATCH_FAILED;

    public static DqlRecoveryReportTypeEnum parse(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        for (DqlRecoveryReportTypeEnum type : values()) {
            if (type.name().equalsIgnoreCase(value)) {
                return type;
            }
        }
        return null;
    }
}
