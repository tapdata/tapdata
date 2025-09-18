package com.tapdata.tm.commons.task.util;

import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/15 11:22 Create
 * @description
 */

public final class AlarmRuleCompare {
    private static final Map<Class<? extends Number>, Compare<? extends Number>> MAPPER = new HashMap<>();
    static {
        MAPPER.put(Double.class, Number::doubleValue);
        MAPPER.put(Float.class, Number::floatValue);
        MAPPER.put(Long.class, Number::longValue);
        MAPPER.put(Integer.class, Number::intValue);
        MAPPER.put(Short.class, Number::shortValue);
        MAPPER.put(Byte.class, Number::byteValue);
    }

    final AlarmRuleDto alarmRule;
    final Compare<? extends Number> compare;

    public AlarmRuleCompare(AlarmRuleDto compareInfo) {
        this.alarmRule = compareInfo;
        if (null == compareInfo || compareInfo.getValue() == null) {
            throw new IllegalArgumentException("compareInfo is null");
        }
        this.compare = MAPPER.get(compareInfo.getValue().getClass());
    }

    public boolean compare(Number value) {
        if (null == value) {
            return false;
        }
        Number current = alarmRule.getValue();
        int compareResult = compare.compareTo(value, current);
        return alarmRule.getEqualsFlag() == compareResult;
    }

    public boolean allCompare(Collection<Number> value) {
        for (Number number : value) {
            if (!compare(number)) {
                return false;
            }
        }
        return true;
    }

    interface Compare<T extends Number> {
        T compare(Number value);

        default int compareTo(Number value, Number current) {
            T left = compare(value);
            T right = compare(current);
            if (left instanceof Comparable c1) {
                return c1.compareTo(right);
            }
            throw new IllegalArgumentException("Not comparable: " + value.getClass());
        }
    }
}
