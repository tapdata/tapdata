package com.tapdata.tm.v2.api.monitor.main.dto;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ChartAndDelayOfApiTest {

    @Nested
    class ItemTest {
        @Test
        void testCreate() {
            ChartAndDelayOfApi.Item item = ChartAndDelayOfApi.Item.create(1000L);
            assertNotNull(item);
            assertEquals(1000L, item.getTs());
        }
    }

    @Nested
    class ChartAndDelayOfApiInstanceTest {
        @Test
        void testCreate() {
            ChartAndDelayOfApi chart = ChartAndDelayOfApi.create();
            assertNotNull(chart);
            assertNotNull(chart.getTs());
            assertNotNull(chart.getRps());
            assertNotNull(chart.getP95());
            assertNotNull(chart.getP99());
            assertNotNull(chart.getMaxDelay());
            assertNotNull(chart.getMinDelay());
            assertNotNull(chart.getRequestCostAvg());
        }
    }
}