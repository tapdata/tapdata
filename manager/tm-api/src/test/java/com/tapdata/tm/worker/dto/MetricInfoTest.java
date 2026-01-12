package com.tapdata.tm.worker.dto;

import com.tapdata.tm.worker.entity.ServerUsage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;

import java.util.Date;

class MetricInfoTest {

    @Nested
    class toUsageTest {
        @org.junit.jupiter.api.Test
        void testNormal() {
            MetricInfo metricInfo = new MetricInfo();
            metricInfo.setCpuUsage(0.5);
            metricInfo.setHeapMemoryUsage(600L);
            metricInfo.setLastUpdateTime(1767888000000L);
            ServerUsage usage = metricInfo.toUsage(metricInfo, "processId", "workOid", ServerUsage.ProcessType.API_SERVER_WORKER);
            Assertions.assertNotNull(usage);
        }

        @org.junit.jupiter.api.Test
        void testLastUpdateTimeIsDate() {
            MetricInfo metricInfo = new MetricInfo();
            metricInfo.setCpuUsage(0.5);
            metricInfo.setHeapMemoryUsage(600L);
            metricInfo.setLastUpdateTime(new Date(1767888005000L));
            ServerUsage usage = metricInfo.toUsage(metricInfo, "processId", "workOid", ServerUsage.ProcessType.API_SERVER_WORKER);
            Assertions.assertNotNull(usage);
        }

        @org.junit.jupiter.api.Test
        void testLastUpdateTimeIsTimeString() {
            MetricInfo metricInfo = new MetricInfo();
            metricInfo.setCpuUsage(0.5);
            metricInfo.setHeapMemoryUsage(600L);
            metricInfo.setLastUpdateTime("1767888060000");
            ServerUsage usage = metricInfo.toUsage(metricInfo, "processId", "workOid", ServerUsage.ProcessType.API_SERVER_WORKER);
            Assertions.assertNotNull(usage);
        }

        @org.junit.jupiter.api.Test
        void testLastUpdateTimeIsTimeString1() {
            MetricInfo metricInfo = new MetricInfo();
            metricInfo.setCpuUsage(0.5);
            metricInfo.setHeapMemoryUsage(600L);
            metricInfo.setLastUpdateTime("1767888060011");
            ServerUsage usage = metricInfo.toUsage(metricInfo, "processId", "workOid", ServerUsage.ProcessType.API_SERVER_WORKER);
            Assertions.assertNotNull(usage);
        }

        @org.junit.jupiter.api.Test
        void testLastUpdateTimeIsNull() {
            MetricInfo metricInfo = new MetricInfo();
            metricInfo.setCpuUsage(0.5);
            metricInfo.setHeapMemoryUsage(600L);
            metricInfo.setLastUpdateTime(null);
            ServerUsage usage = metricInfo.toUsage(metricInfo, "processId", "workOid", ServerUsage.ProcessType.API_SERVER_WORKER);
            Assertions.assertNotNull(usage);
        }
    }
}