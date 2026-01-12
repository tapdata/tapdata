package com.tapdata.tm.worker.entity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;

class ServerUsageMetricTest {

    @Nested
    class InstanceTest {
        @org.junit.jupiter.api.Test
        void testInstance() {
            ServerUsageMetric metric = ServerUsageMetric.instance(1, 1, "1", "1", 0);
            metric.append(null);
            metric.append(new org.bson.Document());
            metric.append(new org.bson.Document("cpuUsage", 0.5));
            metric.append(new org.bson.Document("cpuUsage", 0.5).append("heapMemoryUsage", 600L).append("heapMemoryMax", 1000L).append("lastUpdateTime", 1000L).append("processId", "1").append("workOid", "1").append("processType", 0));
            Assertions.assertEquals(1, metric.getTimeGranularity());
            Assertions.assertEquals(1, metric.getLastUpdateTime());
            Assertions.assertEquals("1", metric.getProcessId());
            Assertions.assertEquals("1", metric.getWorkOid());
            Assertions.assertEquals(0, metric.getProcessType());
        }
    }
}