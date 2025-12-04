package io.tapdata.flow.engine.V2.node.hazelcast.data.batch.rule.increase;

import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.vo.increase.AdjustInfo;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.vo.increase.JudgeResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class LatencyHigherQueueFilledRuleTest {

    @Nested
    class SortTest {
        @Test
        void testSort() {
            LatencyHigherQueueFilledRule rule = new LatencyHigherQueueFilledRule();
            Assertions.assertEquals(20, rule.sort());
        }
    }

    @Nested
    class CheckTest {
        @Test
        void testCheckDelayHigherAndQueueNotFilled() {
            LatencyHigherQueueFilledRule rule = new LatencyHigherQueueFilledRule();
            AdjustInfo adjustInfo = new AdjustInfo("task1");
            adjustInfo.setEventDelay(1500L);
            adjustInfo.setEventDelayThresholdMs(1000L);
            adjustInfo.setEventQueueSize(50);
            adjustInfo.setEventQueueCapacity(100);
            adjustInfo.setEventQueueIdleThreshold(0.7D);
            adjustInfo.setEventQueueFullThreshold(0.95D);

            JudgeResult result = new JudgeResult();
            rule.check(adjustInfo, result);

            Assertions.assertTrue(result.isHasJudge());
            Assertions.assertEquals(1, result.getType());
            Assertions.assertNotNull(result.getReason());
            Assertions.assertNotNull(result.getDetail());
        }

        @Test
        void testCheckDelayHigherButAvailableLessThanRateOf() {
            LatencyHigherQueueFilledRule rule = new LatencyHigherQueueFilledRule();
            AdjustInfo adjustInfo = new AdjustInfo("task1");
            adjustInfo.setEventDelay(1200L);
            adjustInfo.setEventDelayThresholdMs(1000L);
            adjustInfo.setEventQueueSize(65);
            adjustInfo.setEventQueueCapacity(100);
            adjustInfo.setEventQueueIdleThreshold(0.7D);
            adjustInfo.setEventQueueFullThreshold(0.95D);

            JudgeResult result = new JudgeResult();
            result.setRate(0.5);
            rule.check(adjustInfo, result);

            Assertions.assertTrue(result.isHasJudge());
            Assertions.assertTrue(result.isCompleted());
            Assertions.assertNotNull(result.getReason());
        }

        @Test
        void testCheckDelayVeryHighAndQueueFilled() {
            LatencyHigherQueueFilledRule rule = new LatencyHigherQueueFilledRule();
            AdjustInfo adjustInfo = new AdjustInfo("task1");
            adjustInfo.setEventDelay(2000L);
            adjustInfo.setEventDelayThresholdMs(1000L);
            adjustInfo.setEventQueueSize(75);
            adjustInfo.setEventQueueCapacity(100);
            adjustInfo.setEventQueueIdleThreshold(0.7D);
            adjustInfo.setEventQueueFullThreshold(0.95D);

            JudgeResult result = new JudgeResult();
            rule.check(adjustInfo, result);

            Assertions.assertTrue(result.isHasJudge());
            Assertions.assertNotNull(result.getReason());
            Assertions.assertNotNull(result.getDetail());
        }

        @Test
        void testCheckDelayNotHighEnough() {
            LatencyHigherQueueFilledRule rule = new LatencyHigherQueueFilledRule();
            AdjustInfo adjustInfo = new AdjustInfo("task1");
            adjustInfo.setEventDelay(500L);
            adjustInfo.setEventDelayThresholdMs(1000L);
            adjustInfo.setEventQueueSize(50);
            adjustInfo.setEventQueueCapacity(100);

            JudgeResult result = new JudgeResult();
            rule.check(adjustInfo, result);

            Assertions.assertFalse(result.isHasJudge());
        }

        @Test
        void testCheckWithNonZeroResultType() {
            LatencyHigherQueueFilledRule rule = new LatencyHigherQueueFilledRule();
            AdjustInfo adjustInfo = new AdjustInfo("task1");
            adjustInfo.setEventDelay(1500L);
            adjustInfo.setEventDelayThresholdMs(1000L);
            adjustInfo.setEventQueueSize(50);
            adjustInfo.setEventQueueCapacity(100);
            adjustInfo.setEventQueueIdleThreshold(0.7D);
            adjustInfo.setEventQueueFullThreshold(0.95D);

            JudgeResult result = new JudgeResult();
            result.setType(1);
            rule.check(adjustInfo, result);

            Assertions.assertTrue(result.isHasJudge());
            Assertions.assertEquals(1, result.getType());
        }
    }

    @Nested
    class AfterPropertiesSetTest {
        @Test
        void testAfterPropertiesSet() {
            LatencyHigherQueueFilledRule rule = new LatencyHigherQueueFilledRule();
            // Just ensure it doesn't throw exception
            rule.afterPropertiesSet();
        }
    }
}

