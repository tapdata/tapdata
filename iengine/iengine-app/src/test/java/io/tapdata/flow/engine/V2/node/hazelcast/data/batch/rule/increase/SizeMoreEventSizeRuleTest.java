package io.tapdata.flow.engine.V2.node.hazelcast.data.batch.rule.increase;

import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.vo.increase.AdjustInfo;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.vo.increase.JudgeResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SizeMoreEventSizeRuleTest {

    @Nested
    class SortTest {
        @Test
        void testSort() {
            SizeMoreEventSizeRule rule = new SizeMoreEventSizeRule();
            Assertions.assertEquals(0, rule.sort());
        }
    }

    @Nested
    class CheckTest {
        @Test
        void testCheckBatchSizeGreaterThanEventSize() {
            SizeMoreEventSizeRule rule = new SizeMoreEventSizeRule();
            AdjustInfo adjustInfo = new AdjustInfo("task1");
            adjustInfo.setBatchSize(100);
            adjustInfo.setEventSize(50);

            JudgeResult result = new JudgeResult();
            rule.check(adjustInfo, result);

            Assertions.assertTrue(result.isHasJudge());
            Assertions.assertEquals(-2, result.getType());
            Assertions.assertTrue(result.getRate() < 0);
            Assertions.assertEquals(-0.5, result.getRate(), 0.01);
            Assertions.assertNotNull(result.getReason());
            Assertions.assertNotNull(result.getDetail());
        }

        @Test
        void testCheckBatchSizeEqualToEventSize() {
            SizeMoreEventSizeRule rule = new SizeMoreEventSizeRule();
            AdjustInfo adjustInfo = new AdjustInfo("task1");
            adjustInfo.setBatchSize(100);
            adjustInfo.setEventSize(100);

            JudgeResult result = new JudgeResult();
            rule.check(adjustInfo, result);

            Assertions.assertFalse(result.isHasJudge());
        }

        @Test
        void testCheckBatchSizeLessThanEventSize() {
            SizeMoreEventSizeRule rule = new SizeMoreEventSizeRule();
            AdjustInfo adjustInfo = new AdjustInfo("task1");
            adjustInfo.setBatchSize(50);
            adjustInfo.setEventSize(100);

            JudgeResult result = new JudgeResult();
            rule.check(adjustInfo, result);

            Assertions.assertFalse(result.isHasJudge());
        }

        @Test
        void testCheckCalculatesCorrectRate() {
            SizeMoreEventSizeRule rule = new SizeMoreEventSizeRule();
            AdjustInfo adjustInfo = new AdjustInfo("task1");
            adjustInfo.setBatchSize(200);
            adjustInfo.setEventSize(100);

            JudgeResult result = new JudgeResult();
            rule.check(adjustInfo, result);

            Assertions.assertTrue(result.isHasJudge());
            Assertions.assertEquals(-2, result.getType());
            Assertions.assertEquals(-0.5, result.getRate(), 0.01);
        }
    }

    @Nested
    class AfterPropertiesSetTest {
        @Test
        void testAfterPropertiesSet() {
            SizeMoreEventSizeRule rule = new SizeMoreEventSizeRule();
            // Just ensure it doesn't throw exception
            rule.afterPropertiesSet();
        }
    }
}

