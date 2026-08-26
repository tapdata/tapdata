package com.tapdata.tm.dql;

public enum DqlRecoveryBatchStatusEnum {
    CREATED,
    DISPATCHED,
    RUNNING,
    SUCCESS,
    PARTIAL_FAILED,
    FAILED,
    CANCELED;

    public static DqlRecoveryBatchStatusEnum parse(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        for (DqlRecoveryBatchStatusEnum status : values()) {
            if (status.name().equalsIgnoreCase(value)) {
                return status;
            }
        }
        return null;
    }
}
