package io.tapdata.dql.recovery;

import com.tapdata.tm.dql.dto.DqlRecoveryMessageDto;
import io.tapdata.dql.model.DqlPayloadSnapshot;
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
        assertEquals(List.of("EVENT_STARTED", "EVENT_RESULT", "EVENT_STARTED", "EVENT_RESULT", "BATCH_FINISHED"),
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
        assertEquals("FAILED", reports.get(1).getResult());
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
