package com.tapdata.tm.v2.api.monitor.main.entity;

import org.junit.jupiter.api.Nested;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class ApiMetricsRawTest {
    @Nested
    class InstanceTest {
        @org.junit.jupiter.api.Test
        void testInstance() {
            ApiMetricsRaw raw = ApiMetricsRaw.instance("server1", "api1", 1000L, 1);
            assertNotNull(raw);
        }
    }

    @Nested
    class MergeTest {
        @org.junit.jupiter.api.Test
        void testMerge() {
            ApiMetricsRaw raw = ApiMetricsRaw.instance("server1", "api1", 1000L, 0);
            raw.merge(true, 100L, 10L, 10L);
            assertNotNull(raw);
        }

        @org.junit.jupiter.api.Test
        void testMerge1() {
            ApiMetricsRaw raw = ApiMetricsRaw.instance("server1", "api1", 1000L, 1);
            raw.merge(false, 100L, 10L, 10L);
            assertNotNull(raw);
        }

        @org.junit.jupiter.api.Test
        void testMerge2() {
            ApiMetricsRaw raw = ApiMetricsRaw.instance("server1", "api1", 1000L, 2);
            raw.merge(true, 0L, 10L, 10L);
            assertNotNull(raw);
        }
    }
}