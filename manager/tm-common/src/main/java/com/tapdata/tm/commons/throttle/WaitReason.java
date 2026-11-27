package com.tapdata.tm.commons.throttle;

/**
 * Distinct admission or pipeline waiting cause for monitoring and list diagnostics.
 */
public enum WaitReason implements ThrottleCodedEnum {
    NONE(0),
    RATE_WAIT(1),
    SAMPLE_WAIT(2),
    BUFFER_WAIT(3),
    WRITE_IO_WAIT(4),
    LOG_RISK_WAIT(5),
    BACKPRESSURE_WAIT(6);

    private final int code;

    WaitReason(int code) {
        this.code = code;
    }

    @Override
    public int code() {
        return code;
    }

    public static WaitReason fromCode(Integer code) {
        return ThrottleCodedEnum.fromCode(WaitReason.class, code);
    }
}
