package com.tapdata.tm.commons.throttle;

/**
 * Data flow direction enforced independently per phase.
 */
public enum ThrottleDirection implements ThrottleCodedEnum {
    INPUT(1),
    OUTPUT(2);

    private final int code;

    ThrottleDirection(int code) {
        this.code = code;
    }

    @Override
    public int code() {
        return code;
    }

    public static ThrottleDirection fromCode(Integer code) {
        return ThrottleCodedEnum.fromCode(ThrottleDirection.class, code);
    }
}
