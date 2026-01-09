package com.tapdata.tm.v2.api.monitor.main.dto;

import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class ServerTopOnHomepageTest {
    @Nested
    class ItemTest {
        @org.junit.jupiter.api.Test
        void testCreate() {
            ServerTopOnHomepage item = ServerTopOnHomepage.create();
            assertNotNull(item);
            Assertions.assertEquals(0L, item.getTotalRequestCount());
            Assertions.assertEquals(0L, item.getTotalErrorRate());
            Assertions.assertEquals(0D, item.getResponseTimeAvg());
            Assertions.assertEquals(0L, item.getNotHealthyApiCount());
            Assertions.assertEquals(0L, item.getNotHealthyServerCount());
        }
    }

    @Nested
    class mergeTest {
        @org.junit.jupiter.api.Test
        void testMerge() {
            ServerTopOnHomepage item = ServerTopOnHomepage.create();
            ApiMetricsRaw raw = new ApiMetricsRaw();
            raw.setReqCount(100L);
            raw.setErrorCount(10L);
            raw.setDelay(new ArrayList<>());
            raw.setBytes(new ArrayList<>());
            raw.setRps(10.0);
            raw.setP95(1000L);
            item.merge(raw);
            Assertions.assertEquals(100L, item.getTotalRequestCount());
            Assertions.assertEquals(0D, item.getTotalErrorRate());
            Assertions.assertEquals(0D, item.getResponseTimeAvg());
            Assertions.assertEquals(0L, item.getNotHealthyApiCount());
            Assertions.assertEquals(0L, item.getNotHealthyServerCount());
        }
        @org.junit.jupiter.api.Test
        void testMerge2() {
            ServerTopOnHomepage item = ServerTopOnHomepage.create();
            item.merge(null);
            Assertions.assertEquals(0L, item.getTotalRequestCount());
            Assertions.assertEquals(0D, item.getTotalErrorRate());
            Assertions.assertEquals(0D, item.getResponseTimeAvg());
            Assertions.assertEquals(0L, item.getNotHealthyApiCount());
            Assertions.assertEquals(0L, item.getNotHealthyServerCount());
        }
    }
}