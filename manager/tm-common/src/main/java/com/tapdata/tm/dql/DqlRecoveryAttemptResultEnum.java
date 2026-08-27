package com.tapdata.tm.dql;

public enum DqlRecoveryAttemptResultEnum {
    RUNNING,
    SUCCESS,
    FAILED,
    SKIPPED,
    TIMEOUT;

    public static DqlRecoveryAttemptResultEnum parse(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        for (DqlRecoveryAttemptResultEnum result : values()) {
            if (result.name().equalsIgnoreCase(value)) {
                return result;
            }
        }
        return null;
    }
}
