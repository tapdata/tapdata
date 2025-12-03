package io.tapdata.flow.engine.V2.node.hazelcast.data.batch.rule.increase;

import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.vo.increase.AdjustInfo;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.vo.increase.JudgeResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class QueueFillUpLimitRuleTest {

    @Nested
    class SortTest {
        @Test
        void testSort() {
            QueueFillUpLimitRule rule = new QueueFillUpLimitRule();
            Assertions.assertEquals(10, rule.sort());
        }
    }

    @Nested
    class CheckTest {
        @Test
        void testCheckQueueFillUpOverThreshold() {
            QueueFillUpLimitRule rule = new QueueFillUpLimitRule();
            AdjustInfo adjustInfo = new AdjustInfo("task1");
            adjustInfo.setBatchSize(100);
            adjustInfo.setEventQueueSize(96);
            adjustInfo.setEventQueueCapacity(100);
            adjustInfo.setEventQueueFullThreshold(0.95D);

            JudgeResult result = new JudgeResult();
            rule.check(adjustInfo, result);

            Assertions.assertTrue(result.isHasJudge());
            Assertions.assertEquals(-1, result.getType());
            Assertions.assertTrue(result.getRate() < 0);
            Assertions.assertNotNull(result.getReason());
            Assertions.assertNotNull(result.getDetail());
        }

        @Test
        void testCheckQueueNotFillUp() {
            QueueFillUpLimitRule rule = new QueueFillUpLimitRule();
            AdjustInfo adjustInfo = new AdjustInfo("task1");
            adjustInfo.setBatchSize(100);
            adjustInfo.setEventQueueSize(50);
            adjustInfo.setEventQueueCapacity(100);
            adjustInfo.setEventQueueFullThreshold(0.95D);

            JudgeResult result = new JudgeResult();
            rule.check(adjustInfo, result);

            Assertions.assertFalse(result.isHasJudge());
        }

        @Test
        void testCheckBatchSizeIsOne() {
            QueueFillUpLimitRule rule = new QueueFillUpLimitRule();
            AdjustInfo adjustInfo = new AdjustInfo("task1");
            adjustInfo.setBatchSize(1);
            adjustInfo.setEventQueueSize(96);
            adjustInfo.setEventQueueCapacity(100);
            adjustInfo.setEventQueueFullThreshold(0.95D);

            JudgeResult result = new JudgeResult();
            rule.check(adjustInfo, result);

            Assertions.assertFalse(result.isHasJudge());
        }

        @Test
        void testCheckWithExistingRate() {
            QueueFillUpLimitRule rule = new QueueFillUpLimitRule();
            AdjustInfo adjustInfo = new AdjustInfo("task1");
            adjustInfo.setBatchSize(100);
            adjustInfo.setEventQueueSize(98);
            adjustInfo.setEventQueueCapacity(100);
            adjustInfo.setEventQueueFullThreshold(0.95D);

            JudgeResult result = new JudgeResult();
            result.setRate(0.1);
            rule.check(adjustInfo, result);

            Assertions.assertTrue(result.isHasJudge());
            Assertions.assertEquals(-1, result.getType());
            Assertions.assertTrue(result.getRate() < 0.1);
        }
    }

    @Nested
    class AfterPropertiesSetTest {
        @Test
        void testAfterPropertiesSet() {
            QueueFillUpLimitRule rule = new QueueFillUpLimitRule();
            // Just ensure it doesn't throw exception
            rule.afterPropertiesSet();
        }
    }
}

