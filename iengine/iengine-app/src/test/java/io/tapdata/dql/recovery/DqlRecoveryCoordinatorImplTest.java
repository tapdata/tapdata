package io.tapdata.dql.recovery;

import com.tapdata.tm.dql.dto.DqlRecoveryMessageDto;
import com.tapdata.tm.dql.config.DqlRuntimeConfig;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import io.tapdata.dql.model.DqlPayloadSnapshot;
import io.tapdata.dql.model.DqlRecoveryNodeState;
import io.tapdata.dql.model.DqlRecoveryReport;
import io.tapdata.dql.serializer.DqlPayloadSerializer;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlRecoveryCoordinatorImplTest {
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void processesOrderedEventsSeriallyAndDoesNotEnqueueNextBeforeBarrier() throws Exception {
        List<String> operations = new ArrayList<>();
        CountDownLatch firstEnqueued = new CountDownLatch(1);
        CountDownLatch releaseFirstBarrier = new CountDownLatch(1);
        List<DqlRecoveryReport> reports = new ArrayList<>();
        DqlRecoveryEventSource source = eventId -> {
            operations.add("load:" + eventId);
            return completeSnapshot();
        };
        DqlRecoveryEventSink sink = event -> {
            operations.add("enqueue:" + event.getEventId());
            if ("event-1".equals(event.getEventId())) {
                firstEnqueued.countDown();
            }
        };
        DqlRecoveryBarrier barrier = (eventId, timeoutMillis) -> {
            operations.add("barrier:" + eventId);
            if ("event-1".equals(eventId)) {
                assertTrue(releaseFirstBarrier.await(2, TimeUnit.SECONDS));
            }
            return DqlRecoveryBarrier.Outcome.SUCCESS;
        };
        DqlRecoveryCoordinatorImpl coordinator = coordinator(source, sink, barrier, reports,
                () -> false);

        coordinator.start(command("event-1", "event-2"));

        assertTrue(firstEnqueued.await(2, TimeUnit.SECONDS));
        assertFalse(operations.contains("enqueue:event-2"));
        releaseFirstBarrier.countDown();

        assertTrue(coordinator.awaitIdle(2, TimeUnit.SECONDS));
        assertEquals(List.of(
                "load:event-1", "enqueue:event-1", "barrier:event-1",
                "load:event-2", "enqueue:event-2", "barrier:event-2"
        ), operations);
        assertEquals(List.of("BATCH_HEARTBEAT", "EVENT_STARTED", "EVENT_RESULT", "EVENT_STARTED", "EVENT_RESULT", "BATCH_FINISHED"),
                reportTypes(reports));
    }

    @Test
    void continuesAfterFailedEventWhenPolicyAllowsIt() throws Exception {
        List<String> loaded = new ArrayList<>();
        List<DqlRecoveryReport> reports = new ArrayList<>();
        AtomicInteger barrierCalls = new AtomicInteger();
        DqlRecoveryEventSource source = eventId -> {
            loaded.add(eventId);
            return completeSnapshot();
        };
        DqlRecoveryBarrier barrier = (eventId, timeoutMillis) -> barrierCalls.getAndIncrement() == 0
                ? DqlRecoveryBarrier.Outcome.FAILED
                : DqlRecoveryBarrier.Outcome.SUCCESS;
        DqlRecoveryCoordinatorImpl coordinator = coordinator(source, event -> {
        }, barrier, reports, () -> true);

        coordinator.start(command("event-1", "event-2"));

        assertTrue(coordinator.awaitIdle(2, TimeUnit.SECONDS));
        assertEquals(List.of("event-1", "event-2"), loaded);
        assertEquals(List.of("FAILED", "SUCCESS"), eventResults(reports));
        assertEquals("BATCH_FINISHED", reports.get(reports.size() - 1).getType());
    }

    @Test
    void reportsReplayFailureDetailsInsteadOfReplacingThemWithBarrierMessage() throws Exception {
        List<DqlRecoveryReport> reports = new ArrayList<>();
        RuntimeException failure = new RuntimeException("Duplicate entry '2' for key 'idx_unique_order_no'");
        DqlRecoveryBarrier barrier = new DqlRecoveryBarrier() {
            @Override
            public Outcome await(String eventId, long timeoutMillis) {
                return Outcome.FAILED;
            }

            @Override
            public Result awaitResult(String eventId, long timeoutMillis) {
                return new Result(Outcome.FAILED, failure.getMessage(),
                        io.tapdata.exception.ExceptionUtil.getStackString(failure));
            }
        };
        DqlRecoveryCoordinatorImpl coordinator = coordinator(
                eventId -> completeSnapshot(),
                event -> {
                },
                barrier,
                reports,
                () -> false
        );

        coordinator.start(command("batch-original-error", "event-1"));

        assertTrue(coordinator.awaitIdle(2, TimeUnit.SECONDS));
        DqlRecoveryReport eventResult = reports.stream()
                .filter(report -> "EVENT_RESULT".equals(report.getType()))
                .findFirst()
                .orElseThrow();
        assertEquals(failure.getMessage(), eventResult.getMessage());
        assertTrue(eventResult.getErrorDetails().contains(failure.getClass().getName()));
    }

    @Test
    void stopsAfterFailedEventWhenPolicyDisallowsIt() throws Exception {
        List<String> loaded = new ArrayList<>();
        List<DqlRecoveryReport> reports = new ArrayList<>();
        DqlRecoveryCoordinatorImpl coordinator = coordinator(
                eventId -> {
                    loaded.add(eventId);
                    return completeSnapshot();
                },
                event -> {
                },
                (eventId, timeoutMillis) -> DqlRecoveryBarrier.Outcome.FAILED,
                reports,
                () -> false
        );

        coordinator.start(command("event-1", "event-2"));

        assertTrue(coordinator.awaitIdle(2, TimeUnit.SECONDS));
        assertEquals(List.of("event-1"), loaded);
        assertEquals("BATCH_FAILED", reports.get(reports.size() - 1).getType());
        assertEquals("FAILED", reports.stream()
                .filter(report -> "EVENT_RESULT".equals(report.getType()))
                .findFirst()
                .orElseThrow()
                .getResult());
    }

    @Test
    void configConstructorUsesRecoveryTimeoutAndContinuePolicy() throws Exception {
        List<String> loaded = new ArrayList<>();
        DqlRuntimeConfig config = DqlRuntimeConfig.fromMap(Map.of(
                DqlRuntimeConfig.RECOVERY_EVENT_TIMEOUT_SECONDS, "2",
                DqlRuntimeConfig.RECOVERY_CONTINUE_ON_EVENT_FAILURE, "false"));
        DqlRecoveryCoordinatorImpl coordinator = new DqlRecoveryCoordinatorImpl(
                eventId -> {
                    loaded.add(eventId);
                    return completeSnapshot();
                },
                event -> {
                },
                (eventId, timeoutMillis) -> {
                    assertEquals(2_000L, timeoutMillis);
                    return DqlRecoveryBarrier.Outcome.FAILED;
                },
                (command, report) -> {
                },
                executor,
                config
        );

        coordinator.start(command("event-1", "event-2"));

        assertTrue(coordinator.awaitIdle(2, TimeUnit.SECONDS));
        assertEquals(List.of("event-1"), loaded);
    }

    @Test
    void usesRecoveryOnlyRunnerForPausedTaskAndClosesItAfterTheBatch() throws Exception {
        List<String> emitted = new ArrayList<>();
        List<String> closed = new ArrayList<>();
        DqlRecoveryOnlyRunner runner = DqlRecoveryOnlyRunner.open(
                new DqlRecoveryOnlyRunner.TaskSnapshot("task-1", 7L, "paused"),
                new DqlReplaySourceNode() {
                    @Override
                    public void enqueue(com.tapdata.entity.TapdataDqlRecoveryEvent event) {
                        emitted.add(event.getEventId());
                    }

                    @Override
                    public void close() {
                        closed.add("source");
                    }
                },
                () -> closed.add("context")
        );
        DqlRecoveryCoordinatorImpl coordinator = coordinator(
                eventId -> completeSnapshot(),
                event -> {
                    throw new AssertionError("paused task must not use the normal event sink");
                },
                (eventId, timeoutMillis) -> DqlRecoveryBarrier.Outcome.SUCCESS,
                new ArrayList<>(),
                () -> false,
                command -> runner
        );

        coordinator.start(command("event-1", "event-2"));

        assertTrue(coordinator.awaitIdle(2, TimeUnit.SECONDS));
        assertEquals(List.of("event-1", "event-2"), emitted);
        assertEquals(List.of("source", "context"), closed);
        assertFalse(runner.normalSourceStarted());
    }

    @Test
    void usesDagSourceBoundaryForLiveTaskInsteadOfLegacySink() throws Exception {
        DatabaseNode source = new DatabaseNode();
        source.setId("source");
        DAG dag = org.mockito.Mockito.mock(DAG.class);
        org.mockito.Mockito.when(dag.getSourceNodes()).thenReturn(List.of(source));
        List<String> emitted = new ArrayList<>();
        DqlSourceBoundaryInjector injector = new DqlSourceBoundaryInjector(
                dag,
                Map.of("source", event -> emitted.add(event.getEventId()))
        );
        List<DqlRecoveryReport> reports = new ArrayList<>();
        DqlRecoveryReportSender reportSender = (command, report) -> reports.add(report);
        DqlRecoveryCoordinatorImpl coordinator = new DqlRecoveryCoordinatorImpl(
                eventId -> completeSnapshot(),
                event -> {
                    throw new AssertionError("live recovery must use the resolved DAG source boundary");
                },
                (eventId, timeoutMillis) -> DqlRecoveryBarrier.Outcome.SUCCESS,
                reportSender,
                () -> false,
                1000L,
                executor,
                command -> null,
                command -> injector
        );

        coordinator.start(command("event-1", "event-2"));

        assertTrue(coordinator.awaitIdle(2, TimeUnit.SECONDS));
        assertEquals(List.of("event-1", "event-2"), emitted);
        assertEquals("BATCH_FINISHED", reports.get(reports.size() - 1).getType());
    }

    @Test
    void reportsBatchFailureWhenPausedRunnerInitializationFails() throws Exception {
        List<DqlRecoveryReport> reports = new ArrayList<>();
        DqlRecoveryReportSender reportSender = (command, report) -> reports.add(report);
        DqlRecoveryCoordinatorImpl coordinator = new DqlRecoveryCoordinatorImpl(
                eventId -> completeSnapshot(),
                event -> {
                },
                (eventId, timeoutMillis) -> DqlRecoveryBarrier.Outcome.SUCCESS,
                reportSender,
                () -> false,
                1000L,
                executor,
                command -> {
                    throw new IllegalStateException("runner init failed");
                }
        );

        coordinator.start(command("event-1"));

        assertTrue(coordinator.awaitIdle(2, TimeUnit.SECONDS));
        assertEquals(List.of("BATCH_HEARTBEAT", "BATCH_FAILED"), reportTypes(reports));
        assertEquals("runner init failed", reports.get(reports.size() - 1).getMessage());
    }

    @Test
    void restoresLiveSourceBoundaryAndReportsFailureWhenRestorationFails() throws Exception {
        List<DqlRecoveryReport> reports = new ArrayList<>();
        List<String> lifecycle = new ArrayList<>();
        DqlReplaySourceNode sourceBoundary = new DqlReplaySourceNode() {
            @Override
            public void enqueue(com.tapdata.entity.TapdataDqlRecoveryEvent event) {
                lifecycle.add("enqueue");
            }

            @Override
            public void prepareForRecovery(long timeoutMillis) {
                lifecycle.add("prepare");
            }

            @Override
            public void restoreAfterRecovery() {
                lifecycle.add("restore");
                throw new IllegalStateException("source gate restore failed");
            }
        };
        DqlRecoveryBarrier successBarrier = (eventId, timeoutMillis) -> DqlRecoveryBarrier.Outcome.SUCCESS;
        DqlRecoveryCoordinatorImpl coordinator = new DqlRecoveryCoordinatorImpl(
                eventId -> completeSnapshot(),
                event -> {
                    throw new AssertionError("live recovery must use the source boundary");
                },
                successBarrier,
                (command, report) -> reports.add(report),
                () -> false,
                1000L,
                executor,
                command -> null,
                command -> sourceBoundary
        );

        coordinator.start(command("event-1"));

        assertTrue(coordinator.awaitIdle(2, TimeUnit.SECONDS));
        assertEquals(List.of("prepare", "enqueue", "restore"), lifecycle);
        assertEquals("BATCH_FAILED", reports.get(reports.size() - 1).getType());
        assertEquals("source gate restore failed", reports.get(reports.size() - 1).getMessage());
    }

    @Test
    void sendsHeartbeatsWhileTheBatchIsRunningAndStopsAfterCompletion() throws Exception {
        List<DqlRecoveryReport> reports = new ArrayList<>();
        CountDownLatch eventEnqueued = new CountDownLatch(1);
        CountDownLatch releaseBarrier = new CountDownLatch(1);
        ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
        try {
            DqlRuntimeConfig config = DqlRuntimeConfig.fromMap(Map.of(
                    DqlRuntimeConfig.RECOVERY_HEARTBEAT_INTERVAL_SECONDS, "1"));
            DqlRecoveryCoordinatorImpl coordinator = new DqlRecoveryCoordinatorImpl(
                    eventId -> completeSnapshot(),
                    event -> eventEnqueued.countDown(),
                    (eventId, timeoutMillis) -> {
                        assertTrue(releaseBarrier.await(2, TimeUnit.SECONDS));
                        return DqlRecoveryBarrier.Outcome.SUCCESS;
                    },
                    (command, report) -> {
                        synchronized (reports) {
                            reports.add(report);
                        }
                    },
                    () -> false,
                    1000L,
                    executor,
                    command -> null,
                    command -> null,
                    (command, sourceBoundary) -> null,
                    config,
                    heartbeatExecutor
            );

            coordinator.start(command("event-1"));

            assertTrue(eventEnqueued.await(2, TimeUnit.SECONDS));
            assertTrue(awaitReport(reports, "BATCH_HEARTBEAT", 2, TimeUnit.SECONDS));
            assertTrue(reports.stream()
                    .filter(report -> "BATCH_HEARTBEAT".equals(report.getType()))
                    .allMatch(report -> report.getPingTime() != null && report.getPingTime() > 0));

            releaseBarrier.countDown();
            assertTrue(coordinator.awaitIdle(2, TimeUnit.SECONDS));
            int reportCountAfterCompletion;
            synchronized (reports) {
                reportCountAfterCompletion = reports.size();
            }
            Thread.sleep(1_200L);
            synchronized (reports) {
                assertEquals(reportCountAfterCompletion, reports.size());
                assertEquals("BATCH_FINISHED", reports.get(reports.size() - 1).getType());
            }
        } finally {
            heartbeatExecutor.shutdownNow();
        }
    }

    @Test
    void managedRecoveryLoadsNodeMetadataInjectsIntoTemporaryRuntimeAndRestoresIt() throws Exception {
        List<DqlRecoveryReport> reports = new ArrayList<>();
        List<String> enqueued = new ArrayList<>();
        AtomicInteger closeCount = new AtomicInteger();
        DqlRecoveryNodeState hiddenNode = new DqlRecoveryNodeState(
                "unrelated", "Unrelated", false, true, true, null);
        DqlRecoveryBatchRuntime runtime = new DqlRecoveryBatchRuntime() {
            @Override
            public void enqueue(com.tapdata.entity.TapdataDqlRecoveryEvent event) {
                enqueued.add(event.getEventId());
            }

            @Override
            public DqlRecoveryBarrier barrier() {
                return (eventId, timeoutMillis) -> DqlRecoveryBarrier.Outcome.SUCCESS;
            }

            @Override
            public List<DqlRecoveryNodeState> nodeStates() {
                return List.of(hiddenNode);
            }

            @Override
            public void close() {
                closeCount.incrementAndGet();
            }
        };
        DqlRecoveryEvent event = new DqlRecoveryEvent(
                completeSnapshot(), "source", "Source", "failed", "Failed", "target", "Target");
        DqlRecoveryEventSource source = new DqlRecoveryEventSource() {
            @Override
            public DqlPayloadSnapshot load(String eventId) {
                return event.payload();
            }

            @Override
            public DqlRecoveryEvent loadEvent(String eventId) {
                return event;
            }
        };
        DqlRecoveryBatchRuntimeFactory runtimeFactory = (command, events) -> {
            assertEquals(List.of(event), events);
            return runtime;
        };
        DqlRecoveryCoordinatorImpl coordinator = new DqlRecoveryCoordinatorImpl(
                source,
                ignored -> {
                    throw new AssertionError("managed recovery must not use legacy sink");
                },
                (eventId, timeoutMillis) -> {
                    throw new AssertionError("managed recovery must use temporary runtime barrier");
                },
                (command, report) -> reports.add(report),
                () -> false,
                1000L,
                executor,
                command -> null,
                command -> null,
                (command, sourceBoundary) -> null,
                DqlRuntimeConfig.defaults(),
                DqlRecoveryCoordinatorImpl.DEFAULT_HEARTBEAT_EXECUTOR,
                runtimeFactory
        );

        coordinator.start(command("event-1"));

        assertTrue(coordinator.awaitIdle(2, TimeUnit.SECONDS));
        assertEquals(List.of("event-1"), enqueued);
        assertEquals(1, closeCount.get());
        assertEquals("BATCH_FINISHED", reports.get(reports.size() - 1).getType());
        assertEquals(List.of(hiddenNode), reports.get(reports.size() - 1).getNodeStates());
    }

    private boolean awaitReport(List<DqlRecoveryReport> reports,
                                String type,
                                long timeout,
                                TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadline) {
            synchronized (reports) {
                if (reports.stream().anyMatch(report -> type.equals(report.getType()))) {
                    return true;
                }
            }
            Thread.sleep(10L);
        }
        return false;
    }

    private DqlRecoveryCoordinatorImpl coordinator(DqlRecoveryEventSource source,
                                                    DqlRecoveryEventSink sink,
                                                    DqlRecoveryBarrier barrier,
                                                    List<DqlRecoveryReport> reports,
                                                    DqlRecoveryExecutionPolicy policy) {
        return coordinator(source, sink, barrier, reports, policy, command -> null);
    }

    private DqlRecoveryCoordinatorImpl coordinator(DqlRecoveryEventSource source,
                                                    DqlRecoveryEventSink sink,
                                                    DqlRecoveryBarrier barrier,
                                                    List<DqlRecoveryReport> reports,
                                                    DqlRecoveryExecutionPolicy policy,
                                                    DqlRecoveryOnlyRunner.Factory recoveryOnlyRunnerFactory) {
        DqlRecoveryReportSender reportSender = (command, report) -> {
            synchronized (reports) {
                reports.add(report);
            }
        };
        return new DqlRecoveryCoordinatorImpl(
                source,
                sink,
                barrier,
                reportSender,
                policy,
                1000L,
                executor,
                recoveryOnlyRunnerFactory
        );
    }

    private DqlRecoveryMessageDto command(String... eventIds) {
        DqlRecoveryMessageDto command = new DqlRecoveryMessageDto();
        command.setTaskId("task-1");
        command.setBatchId("batch-1");
        command.setTaskVersion(7L);
        command.setOperatorId("operator-1");
        command.setOrderedEventIds(List.of(eventIds));
        return command;
    }

    private DqlPayloadSnapshot completeSnapshot() {
        TapInsertRecordEvent event = TapInsertRecordEvent.create()
                .table("orders")
                .after(Map.of("id", 1));
        return new DqlPayloadSerializer().serialize(event);
    }

    private List<String> reportTypes(List<DqlRecoveryReport> reports) {
        synchronized (reports) {
            return reports.stream().map(DqlRecoveryReport::getType).toList();
        }
    }

    private List<String> eventResults(List<DqlRecoveryReport> reports) {
        synchronized (reports) {
            return reports.stream()
                    .filter(report -> "EVENT_RESULT".equals(report.getType()))
                    .map(DqlRecoveryReport::getResult)
                    .toList();
        }
    }
}
