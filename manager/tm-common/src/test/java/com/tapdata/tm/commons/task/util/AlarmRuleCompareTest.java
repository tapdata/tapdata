package com.tapdata.tm.commons.task.util;

import com.tapdata.tm.commons.task.dto.alarm.AlarmRuleDto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AlarmRuleCompareTest {

    @Test
    void testError() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new AlarmRuleCompare(null));
        AlarmRuleDto compareInfo = new AlarmRuleDto();
        Assertions.assertThrows(IllegalArgumentException.class, () -> new AlarmRuleCompare(compareInfo));
    }

    @Nested
    class CompareTest {
        @Test
        void test() {
            AlarmRuleDto rule = new AlarmRuleDto();
            rule.setValue(100d);
            rule.setEqualsFlag(1);
            AlarmRuleCompare compare = new AlarmRuleCompare(rule);
            assertTrue(compare.compare(100d));
            assertFalse(compare.compare(99d));
            assertTrue(compare.compare(101d));
        }

        @Test
        void testNull() {
            AlarmRuleDto rule = new AlarmRuleDto();
            rule.setValue(100d);
            rule.setEqualsFlag(1);
            AlarmRuleCompare compare = new AlarmRuleCompare(rule);
            assertFalse(compare.compare(null));
        }
    }

    @Nested
    class AllCompareTest {
        @Test
        void test() {
            AlarmRuleDto rule = new AlarmRuleDto();
            rule.setValue(100d);
            rule.setEqualsFlag(1);
            AlarmRuleCompare compare = new AlarmRuleCompare(rule);
            assertTrue(compare.allCompare(List.of(100d, 101d)));
            assertFalse(compare.allCompare(List.of(100d, 99d)));
        }
    }
}