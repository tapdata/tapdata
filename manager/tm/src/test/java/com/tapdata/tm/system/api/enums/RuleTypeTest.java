package com.tapdata.tm.system.api.enums;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RuleTypeTest {

    @Test
    void testOf() {
        assertNull(RuleType.of(null));
        assertEquals(RuleType.SYSTEM, RuleType.of(999));
        assertEquals(RuleType.USER, RuleType.of(0));
    }

}