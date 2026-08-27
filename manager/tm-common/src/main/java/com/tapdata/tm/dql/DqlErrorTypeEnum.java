package com.tapdata.tm.dql;

public enum DqlErrorTypeEnum {
    MALFORMED_RECORD,
    POISON_RECORD,
    TRANSFORM_ERROR,
    TARGET_CONSTRAINT_ERROR,
    UNKNOWN_RECORD_ERROR;

    public static DqlErrorTypeEnum parse(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        if ("TARGET_WRITE_ERROR".equalsIgnoreCase(value)) {
            return TARGET_CONSTRAINT_ERROR;
        }
        for (DqlErrorTypeEnum type : values()) {
            if (type.name().equalsIgnoreCase(value)) {
                return type;
            }
        }
        return null;
    }
}
