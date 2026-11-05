package io.tapdata.flow.engine.V2.node.hazelcast.data.batch;

import io.tapdata.flow.engine.V2.node.hazelcast.data.batch.rule.IncreaseRuleInstance;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.core.async.ThreadPoolExecutorEx;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class AdjustBatchSizeFactoryTest {

    @Nested
    class GetInstanceTest {
        @Test
        void testSingletonInstanceNotNullAndSame() {
            AdjustBatchSizeFactory instance1 = AdjustBatchSizeFactory.getInstance();
            AdjustBatchSizeFactory instance2 = AdjustBatchSizeFactory.getInstance();
            Assertions.assertNotNull(instance1);
            Assertions.assertSame(instance1, instance2);
        }
    }

    @Nested
    class RegisterTest {
        @Test
        void testRegisterWithBlankTaskIdOrNullStage() {
            AdjustBatchSizeFactory.register(null, null, null);
            AdjustBatchSizeFactory.register("", null, null);
            AdjustBatchSizeFactory.register(" ", null, null);
        }

        @Test
        void testRegisterNormal() {
            ThreadPoolExecutorEx sourceRunner = mock(ThreadPoolExecutorEx.class);
            AdjustStage stage = mock(AdjustStage.class);
            when(stage.getNodeId()).thenReturn("node1");

            AdjustBatchSizeFactory.register("task1", stage, sourceRunner);

            AdjustBatchSizeFactory.foreach("task1", Assertions::assertNotNull);
        }
    }

    @Nested
    class StartAndStopTest {
        @Test
        void testStartAndStopNormal() {
            ObsLogger logger = mock(ObsLogger.class);
            AdjustBatchSizeFactory.start("taskStart", logger);
            AdjustBatchSizeFactory.stop("taskStart");
        }

        @Test
        void testStartIgnoresBlankTaskId() {
            AdjustBatchSizeFactory instance = AdjustBatchSizeFactory.getInstance();
            ScheduledExecutorService oldScheduledExecutor =
                    (ScheduledExecutorService) getField(instance, "scheduledExecutor");
            ScheduledExecutorService scheduledExecutor = mock(ScheduledExecutorService.class);
            ObsLogger logger = mock(ObsLogger.class);
            setField(instance, "scheduledExecutor", scheduledExecutor);
            try {
                AdjustBatchSizeFactory.start(" ", logger);

                verify(scheduledExecutor, never())
                        .scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
                verify(logger, never()).info(anyString());
            } finally {
                AdjustBatchSizeFactory.stop(" ");
                setField(instance, "scheduledExecutor", oldScheduledExecutor);
            }
        }

        @Test
        void testStartAllowsMissingLogger() {
            AdjustBatchSizeFactory instance = AdjustBatchSizeFactory.getInstance();
            ScheduledExecutorService oldScheduledExecutor =
                    (ScheduledExecutorService) getField(instance, "scheduledExecutor");
            ScheduledExecutorService scheduledExecutor = mock(ScheduledExecutorService.class);
            ScheduledFuture<?> future = mock(ScheduledFuture.class);
            doReturn(future).when(scheduledExecutor)
                    .scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
            setField(instance, "scheduledExecutor", scheduledExecutor);
            try {
                Assertions.assertDoesNotThrow(() -> AdjustBatchSizeFactory.start("task-null-logger", null));

                verify(scheduledExecutor, times(1))
                        .scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
            } finally {
                AdjustBatchSizeFactory.stop("task-null-logger");
                setField(instance, "scheduledExecutor", oldScheduledExecutor);
            }
        }

        @Test
        void testStopMarksRegisteredManagerNotAliveAfterStart() {
            AdjustBatchSizeFactory instance = AdjustBatchSizeFactory.getInstance();
            ScheduledExecutorService oldScheduledExecutor =
                    (ScheduledExecutorService) getField(instance, "scheduledExecutor");
            ScheduledExecutorService scheduledExecutor = mock(ScheduledExecutorService.class);
            ScheduledFuture<?> future = mock(ScheduledFuture.class);
            doReturn(future).when(scheduledExecutor)
                    .scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class));
            setField(instance, "scheduledExecutor", scheduledExecutor);

            String taskId = "task-reuse-alive";
            AdjustStage stage = mock(AdjustStage.class);
            when(stage.getNodeId()).thenReturn("node-reuse-alive");
            try {
                AdjustBatchSizeFactory.register(taskId, stage, mock(ThreadPoolExecutorEx.class));

                @SuppressWarnings("unchecked")
                Map<String, AdjustBatchSizeFactory.AdjustManager> managerMap =
                        (Map<String, AdjustBatchSizeFactory.AdjustManager>) getField(instance, "adjustInstanceMap");
                AdjustBatchSizeFactory.AdjustManager manager = managerMap.get(taskId);
                Assertions.assertNotNull(manager);

                AdjustBatchSizeFactory.start(taskId, mock(ObsLogger.class));
                AdjustBatchSizeFactory.stop(taskId);

                Assertions.assertFalse(manager.isAlive.get());
            } finally {
                AdjustBatchSizeFactory.unregister(taskId);
                setField(instance, "scheduledExecutor", oldScheduledExecutor);
            }
        }

        @Test
        void testConcurrentStartCancelsSupersededFuture() throws Exception {
            AdjustBatchSizeFactory instance = AdjustBatchSizeFactory.getInstance();
            ScheduledExecutorService oldScheduledExecutor =
                    (ScheduledExecutorService) getField(instance, "scheduledExecutor");
            ScheduledExecutorService scheduledExecutor = mock(ScheduledExecutorService.class);
            ScheduledFuture<?> firstFuture = mock(ScheduledFuture.class);
            ScheduledFuture<?> secondFuture = mock(ScheduledFuture.class);
            AtomicInteger scheduleCount = new AtomicInteger();
            CyclicBarrier scheduleBarrier = new CyclicBarrier(2);
            when(scheduledExecutor.scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any(TimeUnit.class)))
                    .thenAnswer(invocation -> {
                        int index = scheduleCount.getAndIncrement();
                        try {
                            scheduleBarrier.await(500L, TimeUnit.MILLISECONDS);
                        } catch (Exception ignored) {
                            // The synchronized implementation schedules one task at a time.
                        }
                        return index == 0 ? firstFuture : secondFuture;
                    });
            setField(instance, "scheduledExecutor", scheduledExecutor);

            String taskId = "task-concurrent-start";
            ObsLogger logger = mock(ObsLogger.class);
            ExecutorService executorService = Executors.newFixedThreadPool(2);
            CountDownLatch startLatch = new CountDownLatch(2);
            try {
                Runnable startTask = () -> {
                    startLatch.countDown();
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    AdjustBatchSizeFactory.start(taskId, logger);
                };

                Future<?> first = executorService.submit(startTask);
                Future<?> second = executorService.submit(startTask);
                first.get(2L, TimeUnit.SECONDS);
                second.get(2L, TimeUnit.SECONDS);

                verify(firstFuture, times(1)).cancel(true);
            } finally {
                executorService.shutdownNow();
                AdjustBatchSizeFactory.stop(taskId);
                setField(instance, "scheduledExecutor", oldScheduledExecutor);
            }
        }

        @Test
        void testStartScheduleAndExceptionInTask() {
            ObsLogger logger = mock(ObsLogger.class);
            AdjustStage stage = mock(AdjustStage.class);
            when(stage.getNodeId()).thenReturn("node-for-exception");
            ThreadPoolExecutorEx sourceRunner = mock(ThreadPoolExecutorEx.class);
            AdjustBatchSizeFactory.register("task-exception", stage, sourceRunner);

            try (MockedStatic<AdjustBatchSizeFactory> factoryStatic = mockStatic(AdjustBatchSizeFactory.class)) {
                factoryStatic.when(AdjustBatchSizeFactory::getInstance).thenCallRealMethod();
                AdjustBatchSizeFactory realInstance = AdjustBatchSizeFactory.getInstance();

                @SuppressWarnings("unchecked")
                Map<String, ObsLogger> loggerMap = (Map<String, ObsLogger>) getField(realInstance, "taskLoggerMap");
                @SuppressWarnings("unchecked")
                Map<String, java.util.concurrent.ScheduledFuture<?>> futureMap = (Map<String, ScheduledFuture<?>>) getField(realInstance, "adjustTaskFutureMap");
                loggerMap.put("task-exception", logger);

                ScheduledExecutorService scheduledExecutor = mock(ScheduledExecutorService.class);
                ScheduledFuture<?> future = mock(ScheduledFuture.class);
                when(scheduledExecutor.scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), any())).thenAnswer(invocation -> {
                    Runnable runnable = invocation.getArgument(0);
                    runnable.run();
                    return future;
                });
                setField(realInstance, "scheduledExecutor", scheduledExecutor);
                factoryStatic.when(() -> AdjustBatchSizeFactory.start(anyString(), any(ObsLogger.class))).thenCallRealMethod();
                factoryStatic.when(() -> AdjustBatchSizeFactory.stop(anyString())).thenCallRealMethod();
                try {
                    AdjustBatchSizeFactory.start("task-exception", logger);
                    futureMap.put("task-exception", future);
                    doThrow(new RuntimeException("cancel error")).when(future).cancel(true);
                } finally {
                    AdjustBatchSizeFactory.stop("task-exception");
                }

                verify(logger, times(0)).debug(anyString(), any());
            }
        }
    }

    @Nested
    class StopTaskIfNeedTest {
        @Test
        void testStopTaskIfNeedNormalAndException() {
            AdjustBatchSizeFactory instance = AdjustBatchSizeFactory.getInstance();
            @SuppressWarnings("unchecked")
            Map<String, ScheduledFuture<?>> futureMap =
                    (Map<String, ScheduledFuture<?>>) getField(instance, "adjustTaskFutureMap");
            @SuppressWarnings("unchecked")
            Map<String, ObsLogger> loggerMap =
                    (Map<String, ObsLogger>) getField(instance, "taskLoggerMap");

            ScheduledFuture<?> future = mock(ScheduledFuture.class);
            ObsLogger logger = mock(ObsLogger.class);
            futureMap.put("task1", future);
            loggerMap.put("task1", logger);

            AdjustBatchSizeFactory.stopTaskIfNeed("task1");
            verify(future, times(1)).cancel(true);

            futureMap.put("task2", future);
            loggerMap.put("task2", logger);
            doThrow(new RuntimeException("cancel error")).when(future).cancel(true);
            AdjustBatchSizeFactory.stopTaskIfNeed("task2");
            verify(logger, atLeastOnce()).warn(anyString(), any());
        }
    }

    @Nested
    class UnregisterTest {
        @Test
        void testUnregisterBlankTaskId() {
            AdjustBatchSizeFactory.unregister(null);
            AdjustBatchSizeFactory.unregister("");
        }

        @Test
        void testUnregisterNormal() {
            AdjustBatchSizeFactory instance = AdjustBatchSizeFactory.getInstance();
            @SuppressWarnings("unchecked")
            Map<String, AdjustBatchSizeFactory.AdjustManager> map =
                    (Map<String, AdjustBatchSizeFactory.AdjustManager>) getField(instance, "adjustInstanceMap");

            AdjustBatchSizeFactory.AdjustManager manager =
                    new AdjustBatchSizeFactory.AdjustManager(new AtomicBoolean(true), 1000L);
            map.put("task-unregister", manager);

            AdjustBatchSizeFactory.unregister("task-unregister");
            Assertions.assertFalse(map.containsKey("task-unregister"));
        }
    }

    @Nested
    class ForeachTest {
        @Test
        void testForeachBlankTaskId() {
            AdjustBatchSizeFactory.foreach(null, mock(Consumer.class));
            AdjustBatchSizeFactory.foreach("", mock(Consumer.class));
        }

        @Test
        void testForeachNormal() {
            AdjustBatchSizeFactory instance = AdjustBatchSizeFactory.getInstance();
            @SuppressWarnings("unchecked")
            Map<String, AdjustBatchSizeFactory.AdjustManager> map =
                    (Map<String, AdjustBatchSizeFactory.AdjustManager>) getField(instance, "adjustInstanceMap");
            AdjustBatchSizeFactory.AdjustManager manager =
                    new AdjustBatchSizeFactory.AdjustManager(new AtomicBoolean(true), 1000L);
            map.put("task-foreach", manager);

            IncreaseRuleInstance ruleInstance = mock(IncreaseRuleInstance.class);
            manager.nodeList.put("node", ruleInstance);
            @SuppressWarnings("unchecked")
            Consumer<IncreaseRuleInstance> consumer = mock(Consumer.class);

            AdjustBatchSizeFactory.foreach("task-foreach", consumer);
            verify(consumer, atLeastOnce()).accept(any(IncreaseRuleInstance.class));
        }
    }

    @Nested
    class AdjustManagerTest {
        @Test
        void testAppendNullStage() {
            AdjustBatchSizeFactory.AdjustManager manager =
                    new AdjustBatchSizeFactory.AdjustManager(new AtomicBoolean(true), 1000L);
            manager.append("task", null, null);
        }

        @Test
        void testAppendAndForeach() {
            AdjustBatchSizeFactory.AdjustManager manager =
                    new AdjustBatchSizeFactory.AdjustManager(new AtomicBoolean(true), 1000L);
            AdjustStage stage = mock(AdjustStage.class);
            when(stage.getNodeId()).thenReturn("node1");
            ThreadPoolExecutorEx sourceRunner = mock(ThreadPoolExecutorEx.class);

            manager.append("task", stage, sourceRunner);

            Consumer<IncreaseRuleInstance> consumer = mock(Consumer.class);
            manager.foreach(consumer);
            verify(consumer, atLeastOnce()).accept(any(IncreaseRuleInstance.class));
        }
    }

    @Nested
    class LogMethodTest {
        @Test
        void testDebugInfoWarn() {
            AdjustBatchSizeFactory instance = AdjustBatchSizeFactory.getInstance();
            @SuppressWarnings("unchecked")
            Map<String, ObsLogger> loggerMap =
                    (Map<String, ObsLogger>) getField(instance, "taskLoggerMap");
            ObsLogger logger = mock(ObsLogger.class);
            loggerMap.put("task-log", logger);

            AdjustBatchSizeFactory.debug("task-log", "debug-msg", new Object());
            AdjustBatchSizeFactory.info("task-log", "info-msg", new Object());
            AdjustBatchSizeFactory.warn("task-log", "warn-msg", new Object());

            verify(logger, times(1)).debug(anyString(), any());
            verify(logger, times(1)).info(anyString(), any());
            verify(logger, times(1)).warn(anyString(), any());
        }
    }

    private Object getField(Object target, String field) {
        try {
            java.lang.reflect.Field f = target.getClass().getDeclaredField(field);
            f.setAccessible(true);
            return f.get(target);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void setField(Object target, String field, Object value) {
        try {
            java.lang.reflect.Field f = target.getClass().getDeclaredField(field);
            f.setAccessible(true);
            f.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
