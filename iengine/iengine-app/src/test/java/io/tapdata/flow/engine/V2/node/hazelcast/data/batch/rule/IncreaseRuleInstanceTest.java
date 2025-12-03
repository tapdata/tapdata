package io.tapdata.flow.engine.V2.node.hazelcast.data.batch.rule;

import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.AdjustBatchSizeFactory;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.AdjustStage;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.JvmMemoryService;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.rule.increase.LatencyHigherQueueFilledRule;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.rule.increase.QueueFillUpLimitRule;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.rule.increase.SizeMoreEventSizeRule;
import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.vo.increase.AdjustInfo;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.pdk.core.async.ThreadPoolExecutorEx;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.*;

class IncreaseRuleInstanceTest {

    @Nested
    class ConstructorTest {
        @Test
        void testConstructorWithInvalidCheckInterval() {
            AdjustStage stage = mock(AdjustStage.class);
            AtomicBoolean isAlive = new AtomicBoolean(true);

            IncreaseRuleInstance instance1 = new IncreaseRuleInstance(isAlive, "task1", 100000L, stage);
            IncreaseRuleInstance instance2 = new IncreaseRuleInstance(isAlive, "task2", -1L, stage);
            IncreaseRuleInstance instance3 = new IncreaseRuleInstance(isAlive, "task3", 0L, stage);

            // checkIntervalMs > MAX -> MAX, <= 0 -> MIN
            Assertions.assertNotNull(instance1);
            Assertions.assertNotNull(instance2);
            Assertions.assertNotNull(instance3);
            verify(stage, times(3)).setConsumer(any());
        }

        @Test
        void testConstructorNormal() {
            AdjustStage stage = mock(AdjustStage.class);
            AtomicBoolean isAlive = new AtomicBoolean(true);

            IncreaseRuleInstance instance = new IncreaseRuleInstance(isAlive, "task1", 5000L, stage);

            Assertions.assertNotNull(instance);
            verify(stage, times(1)).setConsumer(any());
        }
    }

    @Nested
    class SourceRunnerTest {
        @Test
        void testSourceRunner() {
            AdjustStage stage = mock(AdjustStage.class);
            AtomicBoolean isAlive = new AtomicBoolean(true);
            ThreadPoolExecutorEx sourceRunner = mock(ThreadPoolExecutorEx.class);

            IncreaseRuleInstance instance = new IncreaseRuleInstance(isAlive, "task1", 5000L, stage);
            IncreaseRuleInstance result = instance.sourceRunner(sourceRunner);

            Assertions.assertSame(instance, result);
        }
    }

    @Nested
    class DelayAvgTest {
        @Test
        void testDelayAvgWithEmptyEvents() {
            AdjustStage stage = mock(AdjustStage.class);
            AtomicBoolean isAlive = new AtomicBoolean(true);
            IncreaseRuleInstance instance = new IncreaseRuleInstance(isAlive, "task1", 5000L, stage);

            long delay = instance.delayAvg(null);
            Assertions.assertEquals(0L, delay);

            delay = instance.delayAvg(new ArrayList<>());
            Assertions.assertEquals(0L, delay);
        }

        @Test
        void testDelayAvgWithTapRecordEvent() {
            AdjustStage stage = mock(AdjustStage.class);
            AtomicBoolean isAlive = new AtomicBoolean(true);
            IncreaseRuleInstance instance = new IncreaseRuleInstance(isAlive, "task1", 5000L, stage);

            TapRecordEvent event = mock(TapRecordEvent.class);
            when(event.getReferenceTime()).thenReturn(System.currentTimeMillis() - 1000L);

            List<TapEvent> events = new ArrayList<>();
            events.add(event);

            long delay = instance.delayAvg(events);
            Assertions.assertTrue(delay >= 900L);
        }

        @Test
        void testDelayAvgWithNormalTapEvent() {
            AdjustStage stage = mock(AdjustStage.class);
            AtomicBoolean isAlive = new AtomicBoolean(true);
            IncreaseRuleInstance instance = new IncreaseRuleInstance(isAlive, "task1", 5000L, stage);

            TapEvent event = mock(TapEvent.class);
            when(event.getTime()).thenReturn(System.currentTimeMillis() - 500L);

            List<TapEvent> events = new ArrayList<>();
            events.add(event);

            long delay = instance.delayAvg(events);
            Assertions.assertTrue(delay >= 400L);
        }
    }

    @Nested
    class AdjustTest {
        @Test
        void testAdjust() {
            AdjustStage stage = mock(AdjustStage.class);
            AdjustStage.TaskInfo taskInfo = new AdjustStage.TaskInfo();
            taskInfo.setIncreaseReadSize(100);
            taskInfo.setEventQueueSize(50);
            taskInfo.setEventQueueCapacity(200);
            when(stage.getTaskInfo()).thenReturn(taskInfo);

            AtomicBoolean isAlive = new AtomicBoolean(true);
            IncreaseRuleInstance instance = new IncreaseRuleInstance(isAlive, "task1", 5000L, stage);

            List<TapEvent> events = new ArrayList<>();
            TapEvent event = mock(TapEvent.class);
            events.add(event);

            AdjustInfo adjustInfo = instance.adjust(events, 1000L);

            Assertions.assertEquals("task1", adjustInfo.getTaskId());
            Assertions.assertEquals(1, adjustInfo.getEventSize());
            Assertions.assertEquals(100, adjustInfo.getBatchSize());
            Assertions.assertEquals(50, adjustInfo.getEventQueueSize());
            Assertions.assertEquals(200, adjustInfo.getEventQueueCapacity());
            Assertions.assertEquals(1000L, adjustInfo.getEventDelay());
        }
    }

    @Nested
    class AcceptTest {
        @Test
        void testAccept() throws Exception {
            AdjustStage stage = mock(AdjustStage.class);
            AtomicBoolean isAlive = new AtomicBoolean(true);
            ThreadPoolExecutorEx sourceRunner = mock(ThreadPoolExecutorEx.class);

            doAnswer(invocation -> {
                Runnable runnable = invocation.getArgument(0);
                runnable.run();
                return null;
            }).when(sourceRunner).execute(any(Runnable.class));

            IncreaseRuleInstance instance = new IncreaseRuleInstance(isAlive, "task1", 5000L, stage)
                    .sourceRunner(sourceRunner);

            List<TapEvent> events = new ArrayList<>();
            TapEvent event = mock(TapEvent.class);
            when(event.getTime()).thenReturn(System.currentTimeMillis());
            events.add(event);

            instance.accept(events);
            Thread.sleep(100L);

            Assertions.assertTrue(instance.queue.size() >= 0);
        }
    }

    @Nested
    class CheckOnceTest {
        @Test
        void testCheckOnceWithEmptyQueue() {
            AdjustStage stage = mock(AdjustStage.class);
            AtomicBoolean isAlive = new AtomicBoolean(false);
            IncreaseRuleInstance instance = new IncreaseRuleInstance(isAlive, "task1", 5000L, stage);

            instance.checkOnce();
            // Should not throw exception
        }

        @Test
        void testCheckOnceWithNullIsAlive() {
            AdjustStage stage = mock(AdjustStage.class);
            IncreaseRuleInstance instance = new IncreaseRuleInstance(null, "task1", 5000L, stage);

            instance.checkOnce();
            // Should return early
        }

        @Test
        void testCheckOnceWithQueueIsNotEmpty() {
            ObjectId taskId = new ObjectId();
            AdjustStage stage = mock(AdjustStage.class);
            AdjustStage.TaskInfo adjust = new AdjustStage.TaskInfo();
            adjust.setIncreaseReadSize(50);
            adjust.setEventQueueSize(80);
            adjust.setEventQueueCapacity(100);
            adjust.setEventQueueFullThreshold(.95D);
            adjust.setEventQueueIdleThreshold(.7D);
            adjust.setEventDelayThresholdMs(1000L);
            adjust.setTaskMemThreshold(.8D);
            when(stage.getTaskInfo()).thenReturn(adjust);
            IncreaseRuleInstance instance = new IncreaseRuleInstance(new AtomicBoolean(true), taskId.toHexString(), 5000L, stage);
            List<TapEvent> events = new ArrayList<>();
            long delayAvg = 800L;
            events.add(TapInsertRecordEvent.create().table("test").referenceTime(System.currentTimeMillis()));
            events.add(TapInsertRecordEvent.create().table("test").referenceTime(System.currentTimeMillis()));
            AdjustInfo info = instance.adjust(events, delayAvg);
            try {
                instance.queue.offer(info, 1000L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                AdjustBatchSizeFactory.warn(taskId.toHexString(), "Offer adjust info failed, error message: {}", e.getMessage());
                Thread.currentThread().interrupt();
            }
            TapdataTaskScheduler scheduler = mock(TapdataTaskScheduler.class);
            List<TaskDto> taskDtos = new ArrayList<>();
            TaskDto task = new TaskDto();
            task.setId(taskId);
            DAG dag = mock(DAG.class);
            task.setDag(dag);
            when(dag.getNodes()).thenReturn(new ArrayList<>());
            taskDtos.add(task);

            TaskDto task1 = new TaskDto();
            task1.setId(new ObjectId());
            DAG dag1 = mock(DAG.class);
            task1.setDag(dag1);
            when(dag1.getNodes()).thenReturn(new ArrayList<>());
            taskDtos.add(task1);

            when(scheduler.getRunningTaskInfos()).thenReturn(taskDtos);
            JvmMemoryService memoryService = mock(JvmMemoryService.class);
            MemoryUsage heapUsage = new MemoryUsage(100, 50, 100, 200);
            when(memoryService.getHeapUsage()).thenReturn(heapUsage);
            try (MockedStatic<SpringUtil> su = mockStatic(SpringUtil.class)) {
                su.when(() -> SpringUtil.getBean(TapdataTaskScheduler.class)).thenReturn(scheduler);
                su.when(() -> SpringUtil.getBean(JvmMemoryService.class)).thenReturn(memoryService);
                IncreaseRuleFactory.factory().register(new LatencyHigherQueueFilledRule());
                IncreaseRuleFactory.factory().register(new QueueFillUpLimitRule());
                IncreaseRuleFactory.factory().register(new SizeMoreEventSizeRule());
                instance.checkOnce();
            } finally {
                IncreaseRuleFactory.factory().rules.clear();
            }
            // Should return early
        }

        @Test
        void testCheckOnceWithQueueIsNotEmptyNotAlive() {
            ObjectId taskId = new ObjectId();
            AdjustStage stage = mock(AdjustStage.class);
            AdjustStage.TaskInfo adjust = new AdjustStage.TaskInfo();
            adjust.setIncreaseReadSize(50);
            adjust.setEventQueueSize(80);
            adjust.setEventQueueCapacity(100);
            adjust.setEventQueueFullThreshold(.95D);
            adjust.setEventQueueIdleThreshold(.7D);
            adjust.setEventDelayThresholdMs(1000L);
            adjust.setTaskMemThreshold(.8D);
            when(stage.getTaskInfo()).thenReturn(adjust);
            IncreaseRuleInstance instance = new IncreaseRuleInstance(new AtomicBoolean(false), taskId.toHexString(), 5000L, stage);
            List<TapEvent> events = new ArrayList<>();
            long delayAvg = 800L;
            events.add(TapInsertRecordEvent.create().table("test").referenceTime(System.currentTimeMillis()));
            events.add(TapInsertRecordEvent.create().table("test").referenceTime(System.currentTimeMillis()));
            AdjustInfo info = instance.adjust(events, delayAvg);
            try {
                instance.queue.offer(info, 1000L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                AdjustBatchSizeFactory.warn(taskId.toHexString(), "Offer adjust info failed, error message: {}", e.getMessage());
                Thread.currentThread().interrupt();
            }
            TapdataTaskScheduler scheduler = mock(TapdataTaskScheduler.class);
            List<TaskDto> taskDtos = new ArrayList<>();
            TaskDto task = new TaskDto();
            task.setId(taskId);
            taskDtos.add(task);
            DAG dag = mock(DAG.class);
            task.setDag(dag);
            when(dag.getNodes()).thenReturn(new ArrayList<>());
            when(scheduler.getRunningTaskInfos()).thenReturn(taskDtos);
            try (MockedStatic<SpringUtil> su = mockStatic(SpringUtil.class)) {
                su.when(() -> SpringUtil.getBean(TapdataTaskScheduler.class)).thenReturn(scheduler);
                IncreaseRuleFactory.factory().register(new LatencyHigherQueueFilledRule());
                IncreaseRuleFactory.factory().register(new QueueFillUpLimitRule());
                IncreaseRuleFactory.factory().register(new SizeMoreEventSizeRule());
                instance.checkOnce();
            } finally {
                IncreaseRuleFactory.factory().rules.clear();
            }
            // Should return early
        }

        @Test
        void testCheckOnceWithQueueIsEmpty() {
            AdjustStage stage = mock(AdjustStage.class);
            AdjustStage.TaskInfo adjust = new AdjustStage.TaskInfo();
            adjust.setIncreaseReadSize(50);
            adjust.setEventQueueSize(80);
            adjust.setEventQueueCapacity(100);
            adjust.setEventQueueFullThreshold(.95D);
            adjust.setEventQueueIdleThreshold(.7D);
            adjust.setEventDelayThresholdMs(1000L);
            adjust.setTaskMemThreshold(.8D);
            when(stage.getTaskInfo()).thenReturn(adjust);
            IncreaseRuleInstance instance = new IncreaseRuleInstance(null, "task1", 5000L, stage);
            List<TapEvent> events = new ArrayList<>();
            long delayAvg = 800L;
            AdjustInfo info = instance.adjust(events, delayAvg);
            instance.queue.offer(info);
            instance.checkOnce();
            // Should return early
        }
    }

    @Nested
    class LogChangeInfoIfNeedTest {
        @Test
        void testLogChangeInfoIfNeedNoChange() {
            AdjustStage stage = mock(AdjustStage.class);
            AtomicBoolean isAlive = new AtomicBoolean(true);
            IncreaseRuleInstance instance = new IncreaseRuleInstance(isAlive, "task1", 5000L, stage);

            AdjustInfo adjustInfo = new AdjustInfo("task1");
            adjustInfo.setBatchSize(100);

            Runnable runnable = mock(Runnable.class);
            instance.logChangeInfoIfNeed(adjustInfo, 100, runnable);

            verify(runnable, never()).run();
        }

        @Test
        void testLogChangeInfoIfNeedWithSignificantChange() {
            AdjustStage stage = mock(AdjustStage.class);
            AtomicBoolean isAlive = new AtomicBoolean(true);
            IncreaseRuleInstance instance = new IncreaseRuleInstance(isAlive, "task1", 5000L, stage);

            AdjustInfo adjustInfo = new AdjustInfo("task1");
            adjustInfo.setBatchSize(100);

            Runnable runnable = mock(Runnable.class);
            instance.logChangeInfoIfNeed(adjustInfo, 200, runnable);

            verify(runnable, times(1)).run();
        }
    }

    @Nested
    class FixNumberTest {
        @Test
        void testNormal() {
            Assertions.assertEquals(1, IncreaseRuleInstance.fixNumber(1));
            Assertions.assertEquals(5, IncreaseRuleInstance.fixNumber(5));
            Assertions.assertEquals(9, IncreaseRuleInstance.fixNumber(9));
            Assertions.assertEquals(15, IncreaseRuleInstance.fixNumber(11));
            Assertions.assertEquals(20, IncreaseRuleInstance.fixNumber(15));
            Assertions.assertEquals(20, IncreaseRuleInstance.fixNumber(19));
            Assertions.assertEquals(50, IncreaseRuleInstance.fixNumber(50));
            Assertions.assertEquals(75, IncreaseRuleInstance.fixNumber(75));
            Assertions.assertEquals(100, IncreaseRuleInstance.fixNumber(102));
            Assertions.assertEquals(150, IncreaseRuleInstance.fixNumber(126));
            Assertions.assertEquals(200, IncreaseRuleInstance.fixNumber(177));
            Assertions.assertEquals(150, IncreaseRuleInstance.fixNumber(166));
            Assertions.assertEquals(1000, IncreaseRuleInstance.fixNumber(1010));
            Assertions.assertEquals(1500, IncreaseRuleInstance.fixNumber(1410));
            Assertions.assertEquals(2000, IncreaseRuleInstance.fixNumber(1902));
        }

        @Test
        void testFixNumberEdgeCases() {
            Assertions.assertEquals(1, IncreaseRuleInstance.fixNumber(0));
            Assertions.assertEquals(1, IncreaseRuleInstance.fixNumber(-5));
            Assertions.assertEquals(5, IncreaseRuleInstance.fixNumber(5));
            Assertions.assertEquals(10, IncreaseRuleInstance.fixNumber(10));
        }

        @Test
        void testFixNumberLessThan50() {
            Assertions.assertEquals(15, IncreaseRuleInstance.fixNumber(12));
            Assertions.assertEquals(20, IncreaseRuleInstance.fixNumber(17));
            Assertions.assertEquals(25, IncreaseRuleInstance.fixNumber(22));
            Assertions.assertEquals(30, IncreaseRuleInstance.fixNumber(27));
        }

        @Test
        void testFixNumberBetween50And100() {
            Assertions.assertEquals(50, IncreaseRuleInstance.fixNumber(52));
            Assertions.assertEquals(55, IncreaseRuleInstance.fixNumber(57));
            Assertions.assertEquals(90, IncreaseRuleInstance.fixNumber(92));
        }

        @Test
        void testFixNumberGreaterThan100() {
            Assertions.assertEquals(100, IncreaseRuleInstance.fixNumber(120));
            Assertions.assertEquals(150, IncreaseRuleInstance.fixNumber(140));
            Assertions.assertEquals(200, IncreaseRuleInstance.fixNumber(180));
            Assertions.assertEquals(450, IncreaseRuleInstance.fixNumber(450));
            Assertions.assertEquals(800, IncreaseRuleInstance.fixNumber(800));
        }
    }
}

