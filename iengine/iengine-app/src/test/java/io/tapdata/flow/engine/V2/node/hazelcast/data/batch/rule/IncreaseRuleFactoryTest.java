package io.tapdata.flow.engine.V2.node.hazelcast.data.batch.rule;

import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.vo.increase.AdjustInfo;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.vo.increase.JudgeResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

class IncreaseRuleFactoryTest {

    @Nested
    class FactoryTest {
        @Test
        void testFactorySingleton() {
            IncreaseRuleFactory factory1 = IncreaseRuleFactory.factory();
            IncreaseRuleFactory factory2 = IncreaseRuleFactory.factory();
            Assertions.assertNotNull(factory1);
            Assertions.assertSame(factory1, factory2);
        }
    }

    @Nested
    class RegisterTest {
        private IncreaseRuleFactory factory;

        @BeforeEach
        void setUp() {
            factory = new IncreaseRuleFactory();
        }

        @Test
        void testRegisterNull() {
            factory.register(null);
            Assertions.assertEquals(0, factory.rules.size());
        }

        @Test
        void testRegisterNormalAndSort() {
            AbstractRule rule1 = mock(AbstractRule.class);
            when(rule1.sort()).thenReturn(10);
            AbstractRule rule2 = mock(AbstractRule.class);
            when(rule2.sort()).thenReturn(5);

            factory.register(rule1);
            factory.register(rule2);

            Assertions.assertEquals(2, factory.rules.size());
            // Should be sorted by sort()
            Assertions.assertEquals(5, factory.rules.get(0).sort());
            Assertions.assertEquals(10, factory.rules.get(1).sort());
        }

        @Test
        void testRegisterDuplicateRule() {
            AbstractRule rule = mock(AbstractRule.class);
            when(rule.sort()).thenReturn(10);

            factory.register(rule);
            factory.register(rule);

            Assertions.assertEquals(1, factory.rules.size());
        }
    }

    @Nested
    class LoadOneByOneTest {
        @Test
        void testLoadOneByOne() {
            IncreaseRuleFactory factory = new IncreaseRuleFactory();
            AbstractRule rule1 = mock(AbstractRule.class);
            AbstractRule rule2 = mock(AbstractRule.class);
            when(rule1.sort()).thenReturn(1);
            when(rule2.sort()).thenReturn(2);

            factory.register(rule1);
            factory.register(rule2);

            List<AdjustInfo> adjustInfos = new ArrayList<>();
            adjustInfos.add(new AdjustInfo("task1"));
            AdjustInfo adjustInfo = new AdjustInfo("task2");

            factory.loadOneByOne(adjustInfos, adjustInfo);

            verify(rule1, times(1)).loadAdjustInfo(adjustInfos, adjustInfo);
            verify(rule2, times(1)).loadAdjustInfo(adjustInfos, adjustInfo);
        }
    }

    @Nested
    class EachTest {
        @Test
        void testEachStopsWhenCompleted() {
            IncreaseRuleFactory factory = new IncreaseRuleFactory();
            AbstractRule rule1 = mock(AbstractRule.class);
            AbstractRule rule2 = mock(AbstractRule.class);
            when(rule1.sort()).thenReturn(1);
            when(rule2.sort()).thenReturn(2);

            doAnswer(invocation -> {
                JudgeResult result = invocation.getArgument(1);
                result.setCompleted(true);
                return null;
            }).when(rule1).check(any(AdjustInfo.class), any(JudgeResult.class));

            factory.register(rule1);
            factory.register(rule2);

            AdjustInfo adjustInfo = new AdjustInfo("task1");
            JudgeResult result = new JudgeResult();

            factory.each(adjustInfo, result);

            verify(rule1, times(1)).check(adjustInfo, result);
            verify(rule2, never()).check(any(), any());
        }

        @Test
        void testEachContinuesWhenNotCompleted() {
            IncreaseRuleFactory factory = new IncreaseRuleFactory();
            AbstractRule rule1 = mock(AbstractRule.class);
            AbstractRule rule2 = mock(AbstractRule.class);
            when(rule1.sort()).thenReturn(1);
            when(rule2.sort()).thenReturn(2);

            factory.register(rule1);
            factory.register(rule2);

            AdjustInfo adjustInfo = new AdjustInfo("task1");
            JudgeResult result = new JudgeResult();

            factory.each(adjustInfo, result);

            verify(rule1, times(1)).check(adjustInfo, result);
            verify(rule2, times(1)).check(adjustInfo, result);
        }
    }
}

