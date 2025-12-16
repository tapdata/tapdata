package io.tapdata.flow.engine.V2.node.hazelcast.data.batch.rule;

import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.vo.increase.AdjustInfo;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.vo.increase.JudgeResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class AbstractRuleTest {

    static class TestRule extends AbstractRule {
        @Override
        public int sort() {
            return 100;
        }

        @Override
        public void check(AdjustInfo adjustInfo, JudgeResult result) {
            result.setHasJudge(true);
            result.setRate(0.5);
        }
    }

    @Nested
    class LoadAdjustInfoTest {
        @Test
        void testLoadAdjustInfoDoesNothing() {
            TestRule rule = new TestRule();
            List<AdjustInfo> from = new ArrayList<>();
            from.add(new AdjustInfo("task1"));
            AdjustInfo to = new AdjustInfo("task2");

            // Default implementation does nothing
            rule.loadAdjustInfo(from, to);

            Assertions.assertNotNull(to);
        }
    }

    @Nested
    class SortTest {
        @Test
        void testSort() {
            TestRule rule = new TestRule();
            Assertions.assertEquals(100, rule.sort());
        }
    }

    @Nested
    class CheckTest {
        @Test
        void testCheck() {
            TestRule rule = new TestRule();
            AdjustInfo adjustInfo = new AdjustInfo("task1");
            JudgeResult result = new JudgeResult();

            rule.check(adjustInfo, result);

            Assertions.assertTrue(result.isHasJudge());
            Assertions.assertEquals(0.5, result.getRate());
        }
    }
}

