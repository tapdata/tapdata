package com.tapdata.tm.dql;

public enum DqlEventStatusEnum {
    PENDING,
    REPROCESSING,
    RECOVERED,
    RECOVERY_FAILED,
    NOT_REPROCESSABLE;

    public boolean reprocessable() {
        return this == PENDING || this == RECOVERY_FAILED;
    }

    public static DqlEventStatusEnum parse(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        for (DqlEventStatusEnum status : values()) {
            if (status.name().equalsIgnoreCase(value)) {
                return status;
            }
        }
        return null;
    }
}
