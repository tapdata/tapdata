package com.tapdata.tm.commons.throttle;

/**
 * DML operation category for event-v1 metering and CDC table-operation sampling.
 */
public enum DmlType implements ThrottleCodedEnum {
    INSERT(1),
    UPDATE(2),
    DELETE(3);

    private final int code;

    DmlType(int code) {
        this.code = code;
    }

    @Override
    public int code() {
        return code;
    }

    public static DmlType fromCode(Integer code) {
        return ThrottleCodedEnum.fromCode(DmlType.class, code);
    }
}
