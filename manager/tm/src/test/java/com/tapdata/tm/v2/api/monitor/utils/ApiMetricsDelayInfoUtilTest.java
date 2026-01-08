package com.tapdata.tm.v2.api.monitor.utils;

import org.junit.jupiter.api.Nested;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApiMetricsDelayInfoUtilTest {

    @Nested
    class CheckByCodeTest {
        @org.junit.jupiter.api.Test
        void testCheckByCode() {
            boolean result = ApiMetricsDelayInfoUtil.checkByCode("200");
            assertTrue(result);
        }

        @org.junit.jupiter.api.Test
        void testCheckByCodeWithNon200() {
            boolean result = ApiMetricsDelayInfoUtil.checkByCode("500");
            assertFalse(result);
        }

        @org.junit.jupiter.api.Test
        void testCheckByCodeWithNull() {
            boolean result = ApiMetricsDelayInfoUtil.checkByCode(null);
            assertFalse(result);
        }

        @org.junit.jupiter.api.Test
        void testCheckByCodeWithEmpty() {
            boolean result = ApiMetricsDelayInfoUtil.checkByCode("-");
            assertFalse(result);
        }
    }
}