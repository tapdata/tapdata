package com.tapdata.tm.commons.throttle;

/**
 * Retention risk status for the earliest replayable durable log offset.
 */
public enum LogPositionStatus implements ThrottleCodedEnum {
    NORMAL(0),
    WARN_70(1),
    CRITICAL_90(2),
    UNKNOWN(3),
    EXPIRED(4);

    private final int code;

    LogPositionStatus(int code) {
        this.code = code;
    }

    @Override
    public int code() {
        return code;
    }

    public static LogPositionStatus fromCode(Integer code) {
        return ThrottleCodedEnum.fromCode(LogPositionStatus.class, code);
    }
}
