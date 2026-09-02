package io.tapdata.dql.recovery;

import com.tapdata.entity.TapdataDqlRecoveryEvent;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.dql.dto.DqlRecoveryMessageDto;
import io.tapdata.dql.model.DqlPayloadSnapshot;
import io.tapdata.dql.model.DqlRecoveryReport;
import io.tapdata.dql.serializer.DqlPayloadSerializer;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DqlRecoveryReplayRegressionTest {
    private final DqlPayloadSerializer serializer = new DqlPayloadSerializer();
    private ExecutorService executor;

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void liveReplayPreservesInsertUpdateDeleteOrderAndDoesNotCreateAnotherDqlRecord() throws Exception {
        Map<String, DqlPayloadSnapshot> snapshots = new LinkedHashMap<>();
        snapshots.put("event-insert", serializer.serialize(TapInsertRecordEvent.create()
                .table("orders")
                .after(Map.of("id", 1, "status", "new"))));
        snapshots.put("event-update", serializer.serialize(TapUpdateRecordEvent.create()
                .table("orders")
                .before(Map.of("id", 1, "status", "new"))
                .after(Map.of("id", 1, "status", "paid"))));
        snapshots.put("event-delete", serializer.serialize(TapDeleteRecordEvent.create()
                .table("orders")
                .before(Map.of("id", 1, "status", "paid"))));

        List<Integer> operations = new ArrayList<>();
        List<DqlRecoveryReport> reports = new ArrayList<>();
        DqlReplaySourceNode sourceBoundary = event -> {
            TapRecordEvent record = (TapRecordEvent) event.getTapEvent();
            operations.add(record.getType());
        };
        DqlRecoveryCoordinatorImpl coordinator = new DqlRecoveryCoordinatorImpl(
                snapshots::get,
                event -> {
                    throw new AssertionError("live replay must use the source boundary");
                },
                (eventId, timeoutMillis) -> DqlRecoveryBarrier.Outcome.SUCCESS,
                (command, report) -> reports.add(report),
                () -> false,
                1000L,
                executor,
                command -> null,
                command -> sourceBoundary
        );

        coordinator.start(command("batch-live", "event-insert", "event-update", "event-delete"));

        assertTrue(coordinator.awaitIdle(2, TimeUnit.SECONDS));
        assertEquals(List.of(
                TapInsertRecordEvent.TYPE,
                TapUpdateRecordEvent.TYPE,
                TapDeleteRecordEvent.TYPE
        ), operations);
        assertEquals("BATCH_HEARTBEAT", reports.get(0).getType());
        assertEquals(List.of("EVENT_STARTED", "EVENT_RESULT", "EVENT_STARTED", "EVENT_RESULT",
                "EVENT_STARTED", "EVENT_RESULT", "BATCH_FINISHED"),
                reportTypes(reports).stream()
                        .filter(type -> !"BATCH_HEARTBEAT".equals(type))
                        .toList());
        assertEquals(0, reports.stream().filter(report -> "DQL_EVENT".equals(report.getType())).count());
    }

    @Test
    void pausedReplayKeepsTaskStoppedAndNeverUsesNormalSink() throws Exception {
        List<String> emitted = new ArrayList<>();
        DqlReplaySourceNode replaySource = event -> emitted.add(event.getEventId());
        DqlRecoveryOnlyRunner runner = DqlRecoveryOnlyRunner.open(
                new DqlRecoveryOnlyRunner.TaskSnapshot("task-1", 7L, TaskDto.STATUS_STOP),
                replaySource
        );
        DqlRecoveryCoordinatorImpl coordinator = new DqlRecoveryCoordinatorImpl(
                eventId -> snapshot("orders", eventId),
                event -> {
                    throw new AssertionError("paused replay must not use the normal sink");
                },
                (eventId, timeoutMillis) -> DqlRecoveryBarrier.Outcome.SUCCESS,
                (command, report) -> {
                },
                () -> false,
                1000L,
                executor,
                command -> runner
        );

        coordinator.start(command("batch-paused", "event-1", "event-2"));

        assertTrue(coordinator.awaitIdle(2, TimeUnit.SECONDS));
        assertEquals(List.of("event-1", "event-2"), emitted);
        assertTrue(runner.taskSnapshot().isPaused());
        assertEquals(TaskDto.STATUS_STOP, runner.taskSnapshot().status());
        assertFalse(runner.normalSourceStarted());
    }

    @Test
    void continuesAfterFailureAndTimeoutWhileKeepingServerOrder() throws Exception {
        List<String> emitted = new ArrayList<>();
        List<DqlRecoveryReport> reports = new ArrayList<>();
        Map<String, DqlRecoveryBarrier.Outcome> outcomes = Map.of(
                "event-1", DqlRecoveryBarrier.Outcome.SUCCESS,
                "event-2", DqlRecoveryBarrier.Outcome.FAILED,
                "event-3", DqlRecoveryBarrier.Outcome.TIMEOUT
        );
        DqlRecoveryCoordinatorImpl coordinator = new DqlRecoveryCoordinatorImpl(
                eventId -> snapshot("orders", eventId),
                event -> emitted.add(event.getEventId()),
                (eventId, timeoutMillis) -> outcomes.get(eventId),
                (command, report) -> reports.add(report),
                () -> true,
                1000L,
                executor
        );

        coordinator.start(command("batch-results", "event-1", "event-2", "event-3"));

        assertTrue(coordinator.awaitIdle(2, TimeUnit.SECONDS));
        assertEquals(List.of("event-1", "event-2", "event-3"), emitted);
        assertEquals(List.of("SUCCESS", "FAILED", "TIMEOUT"), eventResults(reports));
        assertEquals("BATCH_FINISHED", reports.get(reports.size() - 1).getType());
    }

    @Test
    void stopsAfterFailureAndRestoresSourceGate() throws Exception {
        DqlSourceReadGate gate = new DqlSourceReadGate();
        List<String> emitted = new ArrayList<>();
        DqlReplaySourceNode sourceBoundary = new DqlReplaySourceNode() {
            @Override
            public void enqueue(TapdataDqlRecoveryEvent event) {
                emitted.add(event.getEventId());
            }

            @Override
            public void prepareForRecovery(long timeoutMillis) throws InterruptedException {
                gate.prepareForRecovery(timeoutMillis);
            }

            @Override
            public void restoreAfterRecovery() {
                gate.restoreAfterRecovery();
            }
        };
        List<DqlRecoveryReport> reports = new ArrayList<>();
        DqlRecoveryCoordinatorImpl coordinator = new DqlRecoveryCoordinatorImpl(
                eventId -> snapshot("orders", eventId),
                event -> {
                    throw new AssertionError("replay must use the source boundary");
                },
                (eventId, timeoutMillis) -> DqlRecoveryBarrier.Outcome.FAILED,
                (command, report) -> reports.add(report),
                () -> false,
                1000L,
                executor,
                command -> null,
                command -> sourceBoundary
        );

        coordinator.start(command("batch-stop", "event-1", "event-2"));

        assertTrue(coordinator.awaitIdle(2, TimeUnit.SECONDS));
        assertEquals(List.of("event-1"), emitted);
        assertEquals("BATCH_FAILED", reports.get(reports.size() - 1).getType());
        assertEquals(DqlSourceReadGate.State.OPEN, gate.getState());
        TapdataEvent normalTraffic = new TapdataEvent();
        assertTrue(gate.allow(normalTraffic));
    }

    @Test
    void duplicateRecoveryMessageAcrossHandlerInstancesStartsOnlyOnce() {
        AtomicInteger starts = new AtomicInteger();
        DqlRecoveryCoordinator coordinator = command -> starts.incrementAndGet();
        DqlRecoveryReportSender reporter = (command, report) -> {
        };
        DqlRecoveryBatchRegistry registry = new DqlRecoveryBatchRegistry();
        DqlRecoveryTaskContextProvider contextProvider = taskId ->
                new DqlRecoveryTaskContext(taskId, 7L, "agent-1");
        DqlRecoveryMessageHandler first = new DqlRecoveryMessageHandler(
                coordinator, reporter, contextProvider, "agent-1", registry);
        DqlRecoveryMessageHandler second = new DqlRecoveryMessageHandler(
                coordinator, reporter, contextProvider, "agent-1", registry);

        assertEquals(DqlRecoveryHandleResult.Outcome.ACCEPTED, first.handle(message("batch-duplicate"))
                .getOutcome());
        assertEquals(DqlRecoveryHandleResult.Outcome.DUPLICATE, second.handle(message("batch-duplicate"))
                .getOutcome());
        assertEquals(1, starts.get());
    }

    private DqlRecoveryMessageDto command(String batchId, String... eventIds) {
        DqlRecoveryMessageDto command = new DqlRecoveryMessageDto();
        command.setTaskId("task-1");
        command.setBatchId(batchId);
        command.setTaskVersion(7L);
        command.setOperatorId("operator-1");
        command.setOrderedEventIds(List.of(eventIds));
        return command;
    }

    private Map<String, Object> message(String batchId) {
        return new LinkedHashMap<>(Map.of(
                "type", "dqlRecovery",
                "taskId", "task-1",
                "batchId", batchId,
                "taskVersion", 7L,
                "orderedEventIds", List.of("event-1", "event-2"),
                "mode", "AUTO"
        ));
    }

    private DqlPayloadSnapshot snapshot(String table, String id) {
        return serializer.serialize(TapInsertRecordEvent.create()
                .table(table)
                .after(Map.of("id", id)));
    }

    private List<String> reportTypes(List<DqlRecoveryReport> reports) {
        return reports.stream().map(DqlRecoveryReport::getType).toList();
    }

    private List<String> eventResults(List<DqlRecoveryReport> reports) {
        return reports.stream()
                .filter(report -> "EVENT_RESULT".equals(report.getType()))
                .map(DqlRecoveryReport::getResult)
                .toList();
    }
}
