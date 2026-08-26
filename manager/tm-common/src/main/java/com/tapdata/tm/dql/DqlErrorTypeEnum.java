package com.tapdata.tm.dql;

public enum DqlErrorTypeEnum {
    MALFORMED_RECORD,
    POISON_RECORD,
    TRANSFORM_ERROR,
    TARGET_WRITE_ERROR,
    UNKNOWN_RECORD_ERROR;

    public static DqlErrorTypeEnum parse(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        for (DqlErrorTypeEnum type : values()) {
            if (type.name().equalsIgnoreCase(value)) {
                return type;
            }
        }
        return null;
    }
}
