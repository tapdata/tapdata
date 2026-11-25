package com.tapdata.tm.commons.throttle;

/**
 * Alignment quality between Agent monotonic window timing and projected UTC wall clock.
 */
public enum TimeQuality implements ThrottleCodedEnum {
    ALIGNED(1),
    DRIFT_DETECTED(2),
    UNALIGNED(3);

    private final int code;

    TimeQuality(int code) {
        this.code = code;
    }

    @Override
    public int code() {
        return code;
    }

    public static TimeQuality fromCode(Integer code) {
        return ThrottleCodedEnum.fromCode(TimeQuality.class, code);
    }
}
