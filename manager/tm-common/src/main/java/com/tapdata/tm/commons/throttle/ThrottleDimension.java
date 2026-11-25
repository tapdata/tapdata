package com.tapdata.tm.commons.throttle;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;

/**
 * Enforced throttling dimension with bitmask representation (BYTES=1, QPS=2).
 */
public enum ThrottleDimension implements ThrottleCodedEnum {
    BYTES(1),
    QPS(2);

    private final int code;

    ThrottleDimension(int code) {
        this.code = code;
    }

    @Override
    public int code() {
        return code;
    }

    public int bitMask() {
        return code;
    }

    public static ThrottleDimension fromCode(Integer code) {
        return ThrottleCodedEnum.fromCode(ThrottleDimension.class, code);
    }

    public static int toMask(Collection<ThrottleDimension> dimensions) {
        if (dimensions == null || dimensions.isEmpty()) {
            return 0;
        }
        int mask = 0;
        for (ThrottleDimension dimension : dimensions) {
            if (dimension != null) {
                mask |= dimension.bitMask();
            }
        }
        return mask;
    }

    public static Set<ThrottleDimension> fromMask(int mask) {
        EnumSet<ThrottleDimension> result = EnumSet.noneOf(ThrottleDimension.class);
        for (ThrottleDimension dimension : values()) {
            if ((mask & dimension.bitMask()) != 0) {
                result.add(dimension);
            }
        }
        return result;
    }
}
