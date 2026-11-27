package com.tapdata.tm.commons.throttle;

/**
 * Sync execution phase subject to independent metering and throttling limits.
 */
public enum ThrottlePhase implements ThrottleCodedEnum {
    FULL(1),
    CDC(2);

    private final int code;

    ThrottlePhase(int code) {
        this.code = code;
    }

    @Override
    public int code() {
        return code;
    }

    public static ThrottlePhase fromCode(Integer code) {
        return ThrottleCodedEnum.fromCode(ThrottlePhase.class, code);
    }
}
