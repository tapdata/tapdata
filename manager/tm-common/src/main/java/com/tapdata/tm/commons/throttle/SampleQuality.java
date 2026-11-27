package com.tapdata.tm.commons.throttle;

/**
 * Quality status of byte estimation for a metered batch or telemetry window.
 */
public enum SampleQuality implements ThrottleCodedEnum {
    VALID(1),
    ACCUMULATING(2),
    STALE_REUSED(3),
    MISSING(4),
    BUDGET_LIMITED(5);

    private final int code;

    SampleQuality(int code) {
        this.code = code;
    }

    @Override
    public int code() {
        return code;
    }

    public static SampleQuality fromCode(Integer code) {
        return ThrottleCodedEnum.fromCode(SampleQuality.class, code);
    }
}
