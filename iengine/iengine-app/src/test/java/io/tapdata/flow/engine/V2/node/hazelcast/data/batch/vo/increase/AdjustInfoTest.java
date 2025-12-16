package io.tapdata.flow.engine.V2.node.hazelcast.data.batch.vo.increase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class AdjustInfoTest {

    @Nested
    class ConstructorTest {
        @Test
        void testConstructor() {
            AdjustInfo adjustInfo = new AdjustInfo("task1");
            Assertions.assertEquals("task1", adjustInfo.getTaskId());
            Assertions.assertTrue(adjustInfo.getTimestamp() > 0);
        }
    }

    @Nested
    class AvgTest {
        @Test
        void testAvgWithEmptyList() {
            List<AdjustInfo> adjustInfos = new ArrayList<>();
            AdjustInfo result = AdjustInfo.avg("task1", adjustInfos);

            Assertions.assertNotNull(result);
            Assertions.assertEquals("task1", result.getTaskId());
        }

        @Test
        void testAvgWithNullList() {
            AdjustInfo result = AdjustInfo.avg("task1", null);

            Assertions.assertNotNull(result);
            Assertions.assertEquals("task1", result.getTaskId());
        }

        @Test
        void testAvgWithSingleItem() {
            List<AdjustInfo> adjustInfos = new ArrayList<>();
            AdjustInfo info1 = new AdjustInfo("task1");
            info1.setEventSize(100);
            info1.setBatchSize(50);
            info1.setEventDelay(1000L);
            adjustInfos.add(info1);

            AdjustInfo result = AdjustInfo.avg("task1", adjustInfos);

            Assertions.assertSame(info1, result);
        }

        @Test
        void testAvgWithMultipleItems() {
            List<AdjustInfo> adjustInfos = new ArrayList<>();

            AdjustInfo info1 = new AdjustInfo("task1");
            info1.setEventSize(100);
            info1.setBatchSize(50);
            info1.setEventQueueSize(10);
            info1.setEventQueueCapacity(100);
            info1.setEventQueueFullThreshold(0.95D);
            info1.setEventQueueIdleThreshold(0.7D);
            info1.setEventDelay(1000L);
            info1.setEventDelayThresholdMs(500L);
            info1.setEventMem(1000L);
            info1.setEventMemAvg(100L);
            info1.setTaskMemThreshold(0.8D);

            AdjustInfo info2 = new AdjustInfo("task1");
            info2.setEventSize(200);
            info2.setBatchSize(60);
            info2.setEventQueueSize(20);
            info2.setEventQueueCapacity(200);
            info2.setEventQueueFullThreshold(0.9D);
            info2.setEventQueueIdleThreshold(0.6D);
            info2.setEventDelay(2000L);
            info2.setEventDelayThresholdMs(600L);
            info2.setEventMem(2000L);
            info2.setEventMemAvg(200L);
            info2.setTaskMemThreshold(0.75D);

            adjustInfos.add(info1);
            adjustInfos.add(info2);

            AdjustInfo result = AdjustInfo.avg("task1", adjustInfos);

            Assertions.assertEquals("task1", result.getTaskId());
            Assertions.assertEquals(150, result.getEventSize());
            Assertions.assertEquals(60, result.getBatchSize());
            Assertions.assertEquals(20, result.getEventQueueSize());
            Assertions.assertEquals(200, result.getEventQueueCapacity());
            Assertions.assertEquals(0.9D, result.getEventQueueFullThreshold());
            Assertions.assertEquals(0.6D, result.getEventQueueIdleThreshold());
            Assertions.assertEquals(2000L, result.getEventDelay());
            Assertions.assertEquals(600L, result.getEventDelayThresholdMs());
            Assertions.assertEquals(1500L, result.getEventMem());
            Assertions.assertEquals(150L, result.getEventMemAvg());
            Assertions.assertEquals(0.75D, result.getTaskMemThreshold());
        }

        @Test
        void testAvgWithThreeItems() {
            List<AdjustInfo> adjustInfos = new ArrayList<>();

            AdjustInfo info1 = new AdjustInfo("task1");
            info1.setEventSize(100);
            info1.setEventMem(1000L);
            info1.setEventMemAvg(100L);
            info1.setEventDelay(1000L);

            AdjustInfo info2 = new AdjustInfo("task1");
            info2.setEventSize(200);
            info2.setEventMem(2000L);
            info2.setEventMemAvg(200L);
            info2.setEventDelay(500L);

            AdjustInfo info3 = new AdjustInfo("task1");
            info3.setEventSize(300);
            info3.setEventMem(3000L);
            info3.setEventMemAvg(300L);
            info3.setEventDelay(1500L);

            adjustInfos.add(info1);
            adjustInfos.add(info2);
            adjustInfos.add(info3);

            AdjustInfo result = AdjustInfo.avg("task1", adjustInfos);

            Assertions.assertEquals(200, result.getEventSize());
            Assertions.assertEquals(2000L, result.getEventMem());
            Assertions.assertEquals(200L, result.getEventMemAvg());
            Assertions.assertEquals(1500L, result.getEventDelay());
        }
    }

    @Nested
    class GetterSetterTest {
        @Test
        void testAllGettersAndSetters() {
            AdjustInfo adjustInfo = new AdjustInfo("task1");

            adjustInfo.setEventSize(100);
            adjustInfo.setBatchSize(50);
            adjustInfo.setEventQueueSize(10);
            adjustInfo.setEventQueueCapacity(100);
            adjustInfo.setEventQueueFullThreshold(0.95D);
            adjustInfo.setEventQueueIdleThreshold(0.7D);
            adjustInfo.setEventDelay(1000L);
            adjustInfo.setEventDelayThresholdMs(500L);
            adjustInfo.setCurrentTaskNodes(5);
            adjustInfo.setAllTaskNodes(20);
            adjustInfo.setAllTaskCount(10);
            adjustInfo.setEventMem(1000L);
            adjustInfo.setEventMemAvg(100L);
            adjustInfo.setSysMem(10000L);
            adjustInfo.setSysMemUsed(5000L);
            adjustInfo.setTaskMemThreshold(0.8D);

            Assertions.assertEquals(100, adjustInfo.getEventSize());
            Assertions.assertEquals(50, adjustInfo.getBatchSize());
            Assertions.assertEquals(10, adjustInfo.getEventQueueSize());
            Assertions.assertEquals(100, adjustInfo.getEventQueueCapacity());
            Assertions.assertEquals(0.95D, adjustInfo.getEventQueueFullThreshold());
            Assertions.assertEquals(0.7D, adjustInfo.getEventQueueIdleThreshold());
            Assertions.assertEquals(1000L, adjustInfo.getEventDelay());
            Assertions.assertEquals(500L, adjustInfo.getEventDelayThresholdMs());
            Assertions.assertEquals(5, adjustInfo.getCurrentTaskNodes());
            Assertions.assertEquals(20, adjustInfo.getAllTaskNodes());
            Assertions.assertEquals(10, adjustInfo.getAllTaskCount());
            Assertions.assertEquals(1000L, adjustInfo.getEventMem());
            Assertions.assertEquals(100L, adjustInfo.getEventMemAvg());
            Assertions.assertEquals(10000L, adjustInfo.getSysMem());
            Assertions.assertEquals(5000L, adjustInfo.getSysMemUsed());
            Assertions.assertEquals(0.8D, adjustInfo.getTaskMemThreshold());
        }
    }
}

