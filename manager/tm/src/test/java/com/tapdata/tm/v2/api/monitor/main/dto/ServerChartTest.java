package com.tapdata.tm.v2.api.monitor.main.dto;

import com.tapdata.tm.worker.entity.ServerUsage;
import com.tapdata.tm.worker.entity.ServerUsageMetric;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class ServerChartTest {

    @Nested
    class UsageTest {
        @Test
        void testCreate() {
            ServerChart.Usage usage = ServerChart.Usage.create();
            assertNotNull(usage);
            assertNotNull(usage.getCpuUsage());
            assertNotNull(usage.getMemoryUsage());
            assertNotNull(usage.getTs());
        }

        @Test
        void testAddEmpty() {
            ServerChart.Usage usage = ServerChart.Usage.create();
            usage.addEmpty(1000L, true);
            assertNotNull(usage);
            assertNotNull(usage.getCpuUsage());
            assertNotNull(usage.getMemoryUsage());
            assertNotNull(usage.getTs());
        }

        @Test
        void testAdd() {
            ServerChart.Usage usage = ServerChart.Usage.create();
            ServerUsage usage1 = ServerUsage.instance(1000L, "processId", "workOid", 0);
            usage1.setCpuUsage(1.0D);
            usage1.setHeapMemoryUsage(100L);
            usage1.setHeapMemoryMax(200L);
            usage.add(usage1);
            assertNotNull(usage);
            assertNotNull(usage.getCpuUsage());
            assertNotNull(usage.getMemoryUsage());
            assertNotNull(usage.getTs());
        }

        @Test
        void testAddWithNullHeapMemoryMax() {
            ServerChart.Usage usage = ServerChart.Usage.create();
            ServerUsageMetric usage1 = ServerUsageMetric.instance(0, 1000L, "processId", "workOid", 0);
            usage1.setCpuUsage(1.0D);
            usage1.setHeapMemoryUsage(100L);
            usage1.setHeapMemoryMax(null);
            usage.add(usage1);
            assertNotNull(usage);
            assertNotNull(usage.getCpuUsage());
            assertNotNull(usage.getMemoryUsage());
            assertNotNull(usage.getTs());
        }

        @Test
        void testAddServerUsageMetric() {
            ServerChart.Usage usage = ServerChart.Usage.create();
            ServerUsageMetric usage1 = ServerUsageMetric.instance(0,1000L, "processId", "workOid", 0);
            usage1.setCpuUsage(1.0D);
            usage1.setHeapMemoryUsage(0L);
            usage1.setHeapMemoryMax(100L);
            usage.add(usage1);
            usage.add(usage1);
            assertNotNull(usage);
            assertNotNull(usage.getCpuUsage());
            assertNotNull(usage.getMemoryUsage());
            assertNotNull(usage.getTs());
        }

        @Test
        void testAddServerUsageMetric2() {
            ServerChart.Usage usage = ServerChart.Usage.create();
            ServerUsageMetric usage1 = ServerUsageMetric.instance(0,1000L, "processId", "workOid", 0);
            usage1.setCpuUsage(1.0D);
            usage1.setHeapMemoryUsage(0L);
            usage1.setHeapMemoryMax(100L);
            usage1.setMaxCpuUsage(1.0D);
            usage1.setMinCpuUsage(1.0D);
            usage1.setMaxHeapMemoryUsage(100L);
            usage1.setMinHeapMemoryUsage(100L);
            usage.add(usage1);
            usage.add(usage1);
            assertNotNull(usage);
            assertNotNull(usage.getCpuUsage());
            assertNotNull(usage.getMemoryUsage());
            assertNotNull(usage.getTs());
        }

        @Test
        void testInitStatisticsData() {
            ServerChart.Usage usage = ServerChart.Usage.create();
            usage.initStatisticsData();
            assertNotNull(usage);
            assertNotNull(usage.getMaxCpuUsage());
            assertNotNull(usage.getMinCpuUsage());
            assertNotNull(usage.getMaxMemoryUsage());
            assertNotNull(usage.getMinMemoryUsage());
        }

        @Test
        void testAddEmptyWithoutStatistics() {
            ServerChart.Usage usage = ServerChart.Usage.create();
            usage.addEmpty(1000L, false);
            assertNotNull(usage);
            assertNotNull(usage.getCpuUsage());
            assertNotNull(usage.getMemoryUsage());
            assertNotNull(usage.getTs());
        }

        @Test
        void testAddEmptyWithStatistics() {
            ServerChart.Usage usage = ServerChart.Usage.create();
            usage.addEmpty(1000L, true);
            assertNotNull(usage);
            assertNotNull(usage.getCpuUsage());
            assertNotNull(usage.getMemoryUsage());
            assertNotNull(usage.getTs());
        }
    }

    @Nested
    class RequestTest {
        @Test
        void testCreate() {
            ServerChart.Request request = ServerChart.Request.create();
            assertNotNull(request);
            assertNotNull(request.getRequestCount());
            assertNotNull(request.getErrorRate());
            assertNotNull(request.getTs());
        }
    }

    @Nested
    class DelayTest {
        @Test
        void testCreate() {
            ServerChart.Delay delay = ServerChart.Delay.create();
            assertNotNull(delay);
            assertNotNull(delay.getAvg());
            assertNotNull(delay.getP95());
            assertNotNull(delay.getP99());
            assertNotNull(delay.getMaxDelay());
            assertNotNull(delay.getMinDelay());
            assertNotNull(delay.getTs());
        }
    }

    @Nested
    class ItemTest {
        @Test
        void testCreate() {
            ServerChart.Item item = ServerChart.Item.create(1000L);
            assertNotNull(item);
            assertNotNull(item.getTs());
        }
    }
}