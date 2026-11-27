package com.tapdata.tm.commons.throttle;

/**
 * Enum with a stable integer code for telemetry serialization and cross-version compatibility.
 */
interface ThrottleCodedEnum {

    int code();

    static <E extends Enum<E> & ThrottleCodedEnum> E fromCode(Class<E> enumType, Integer code) {
        if (code == null) {
            return null;
        }
        for (E candidate : enumType.getEnumConstants()) {
            if (candidate.code() == code) {
                return candidate;
            }
        }
        return null;
    }
}
