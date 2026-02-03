package com.tapdata.tm.v2.api.monitor.main.entity;

import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import com.tapdata.tm.apiServer.enums.TimeGranularity;
import org.junit.jupiter.api.Nested;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class ApiMetricsRawTest {
    @Nested
    class InstanceTest {
        @org.junit.jupiter.api.Test
        void testInstance() {
            ApiMetricsRaw raw = ApiMetricsRaw.instance("server1", "api1", "api1", 1000L, TimeGranularity.MINUTE, MetricTypes.API_SERVER);
            assertNotNull(raw);
        }
    }

    @Nested
    class MergeTest {
        @org.junit.jupiter.api.Test
        void testMerge() {
            ApiMetricsRaw raw = ApiMetricsRaw.instance("server1", "api1", "api1", 1000L, TimeGranularity.SECOND_FIVE, MetricTypes.API_SERVER);
            raw.merge(true, 100L, 10L, 10L);
            assertNotNull(raw);
        }

        @org.junit.jupiter.api.Test
        void testMerge1() {
            ApiMetricsRaw raw = ApiMetricsRaw.instance("server1", "api1", "api1", 1000L, TimeGranularity.MINUTE, MetricTypes.API_SERVER);
            raw.merge(false, 100L, 10L, 10L);
            assertNotNull(raw);
        }

        @org.junit.jupiter.api.Test
        void testMerge2() {
            ApiMetricsRaw raw = ApiMetricsRaw.instance("server1", "api1", "api1", 1000L, TimeGranularity.HOUR, MetricTypes.API_SERVER);
            raw.merge(true, 0L, 10L, 10L);
            assertNotNull(raw);
        }
    }
}