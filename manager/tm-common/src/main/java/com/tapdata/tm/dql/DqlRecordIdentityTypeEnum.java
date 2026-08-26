package com.tapdata.tm.dql;

/**
 * Describes how Engine built the stable business-record identity used for DLQ overwrite-risk detection.
 */
public enum DqlRecordIdentityTypeEnum {
    PRIMARY_KEY,
    UNIQUE_INDEX,
    FULL_FIELD_HASH,
    UNKNOWN;

    public static DqlRecordIdentityTypeEnum parse(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        for (DqlRecordIdentityTypeEnum type : values()) {
            if (type.name().equalsIgnoreCase(value)) {
                return type;
            }
        }
        return null;
    }
}
