package io.tapdata.dql.recovery;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

class DqlRecoveryMessageHandlerTest {
    private static final String TASK_ID = "task-1";
    private static final String BATCH_ID = "batch-1";
    private static final String AGENT_ID = "agent-1";

    @Test
    @DisplayName("accepts a valid recovery message and reports BATCH_STARTED once")
    void acceptsValidMessage() {
        DqlRecoveryCoordinator coordinator = mock(DqlRecoveryCoordinator.class);
        DqlRecoveryReportSender reporter = mock(DqlRecoveryReportSender.class);
        DqlRecoveryTaskContextProvider contextProvider = contextProvider();
        DqlRecoveryMessageHandler handler = new DqlRecoveryMessageHandler(coordinator, reporter, contextProvider, AGENT_ID);

        DqlRecoveryHandleResult result = handler.handle(message());

        assertEquals(DqlRecoveryHandleResult.Outcome.ACCEPTED, result.getOutcome());
        assertEquals(BATCH_ID, result.getBatchId());
        verify(coordinator).start(result.getCommand());
        verify(reporter).reportBatchStarted(result.getCommand());
    }

    @Test
    @DisplayName("reports BATCH_STARTED before the coordinator can report EVENT_STARTED")
    void reportsBatchStartedBeforeCoordinatorCanReportEventStarted() {
        List<String> calls = new java.util.ArrayList<>();
        DqlRecoveryReportSender reporter = (command, report) -> calls.add(report.getType());
        DqlRecoveryCoordinator coordinator = command -> {
            calls.add("COORDINATOR_STARTED");
            reporter.reportEventStarted(command, "event-1", "attempt-1", 1L);
        };
        DqlRecoveryMessageHandler handler = new DqlRecoveryMessageHandler(
                coordinator, reporter, contextProvider(), AGENT_ID);

        DqlRecoveryHandleResult result = handler.handle(message());

        assertEquals(DqlRecoveryHandleResult.Outcome.ACCEPTED, result.getOutcome());
        assertEquals(List.of("BATCH_STARTED", "COORDINATOR_STARTED", "EVENT_STARTED"), calls);
    }

    @Test
    @DisplayName("reports BATCH_FAILED when coordinator startup fails after BATCH_STARTED")
    void reportsBatchFailureWhenCoordinatorStartupFails() {
        List<String> calls = new java.util.ArrayList<>();
        DqlRecoveryReportSender reporter = (command, report) -> calls.add(report.getType());
        DqlRecoveryCoordinator coordinator = command -> {
            throw new IllegalStateException("runner unavailable");
        };
        DqlRecoveryMessageHandler handler = new DqlRecoveryMessageHandler(
                coordinator, reporter, contextProvider(), AGENT_ID);

        DqlRecoveryHandleResult failed = handler.handle(message());
        DqlRecoveryHandleResult duplicate = handler.handle(message());

        assertEquals(DqlRecoveryHandleResult.Outcome.REJECTED, failed.getOutcome());
        assertEquals(DqlRecoveryHandleResult.Outcome.DUPLICATE, duplicate.getOutcome());
        assertEquals(List.of("BATCH_STARTED", "BATCH_FAILED"), calls);
    }

    @Test
    @DisplayName("reports BATCH_FAILED when the recovery coordinator is not configured")
    void reportsBatchFailureWhenCoordinatorIsUnavailable() {
        DqlRecoveryReportSender reporter = mock(DqlRecoveryReportSender.class);
        DqlRecoveryMessageHandler handler = new DqlRecoveryMessageHandler(
                null, reporter, contextProvider(), AGENT_ID);

        DqlRecoveryHandleResult failed = handler.handle(message());

        assertEquals(DqlRecoveryHandleResult.Outcome.REJECTED, failed.getOutcome());
        assertTrue(failed.getMessage().contains("coordinator"));
        verify(reporter).reportBatchFailed(org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.contains("coordinator"),
                org.mockito.ArgumentMatchers.anyLong());
    }

    @Test
    @DisplayName("rejects an invalid message without starting recovery or reporting a callback")
    void rejectsInvalidMessage() {
        DqlRecoveryCoordinator coordinator = mock(DqlRecoveryCoordinator.class);
        DqlRecoveryReportSender reporter = mock(DqlRecoveryReportSender.class);
        DqlRecoveryMessageHandler handler = new DqlRecoveryMessageHandler(coordinator, reporter, contextProvider(), AGENT_ID);
        Map<String, Object> invalid = message();
        invalid.put("orderedEventIds", List.of("event-1", "event-1"));

        DqlRecoveryHandleResult result = handler.handle(invalid);

        assertEquals(DqlRecoveryHandleResult.Outcome.REJECTED, result.getOutcome());
        assertTrue(result.getMessage().contains("orderedEventIds"));
        verify(coordinator, never()).start(org.mockito.ArgumentMatchers.any());
        verify(reporter, never()).reportBatchStarted(org.mockito.ArgumentMatchers.any());
    }

    @Test
    @DisplayName("rejects a message for a different agent or task version")
    void rejectsWrongTaskContext() {
        DqlRecoveryMessageHandler handler = new DqlRecoveryMessageHandler(
                mock(DqlRecoveryCoordinator.class),
                mock(DqlRecoveryReportSender.class),
                taskId -> new DqlRecoveryTaskContext(taskId, 7L, "another-agent"),
                AGENT_ID
        );

        DqlRecoveryHandleResult result = handler.handle(message());

        assertEquals(DqlRecoveryHandleResult.Outcome.REJECTED, result.getOutcome());
        assertTrue(result.getMessage().contains("agent"));
    }

    @Test
    @DisplayName("treats a concurrent duplicate batch message as an idempotent rejection")
    void rejectsDuplicateBatch() {
        DqlRecoveryCoordinator coordinator = mock(DqlRecoveryCoordinator.class);
        DqlRecoveryReportSender reporter = mock(DqlRecoveryReportSender.class);
        DqlRecoveryMessageHandler handler = new DqlRecoveryMessageHandler(coordinator, reporter, contextProvider(), AGENT_ID);

        DqlRecoveryHandleResult first = handler.handle(message());
        DqlRecoveryHandleResult duplicate = handler.handle(message());

        assertEquals(DqlRecoveryHandleResult.Outcome.ACCEPTED, first.getOutcome());
        assertEquals(DqlRecoveryHandleResult.Outcome.DUPLICATE, duplicate.getOutcome());
        assertFalse(duplicate.isStarted());
        verify(coordinator).start(first.getCommand());
        verify(reporter).reportBatchStarted(first.getCommand());
    }

    @Test
    @DisplayName("keeps batch idempotency across websocket handler instances")
    void sharesBatchClaimAcrossHandlerInstances() {
        DqlRecoveryBatchRegistry registry = new DqlRecoveryBatchRegistry();
        DqlRecoveryCoordinator firstCoordinator = mock(DqlRecoveryCoordinator.class);
        DqlRecoveryCoordinator secondCoordinator = mock(DqlRecoveryCoordinator.class);
        DqlRecoveryReportSender firstReporter = mock(DqlRecoveryReportSender.class);
        DqlRecoveryReportSender secondReporter = mock(DqlRecoveryReportSender.class);
        DqlRecoveryMessageHandler firstHandler = new DqlRecoveryMessageHandler(
                firstCoordinator, firstReporter, contextProvider(), AGENT_ID, registry);
        DqlRecoveryMessageHandler secondHandler = new DqlRecoveryMessageHandler(
                secondCoordinator, secondReporter, contextProvider(), AGENT_ID, registry);

        DqlRecoveryHandleResult first = firstHandler.handle(message());
        DqlRecoveryHandleResult duplicate = secondHandler.handle(message());

        assertEquals(DqlRecoveryHandleResult.Outcome.ACCEPTED, first.getOutcome());
        assertEquals(DqlRecoveryHandleResult.Outcome.DUPLICATE, duplicate.getOutcome());
        verify(secondCoordinator, never()).start(org.mockito.ArgumentMatchers.any());
        verify(secondReporter, never()).reportBatchStarted(org.mockito.ArgumentMatchers.any());
    }

    @Test
    @DisplayName("fails the batch after coordinator startup fails")
    void failsBatchAfterCoordinatorFailure() {
        DqlRecoveryCoordinator coordinator = mock(DqlRecoveryCoordinator.class);
        DqlRecoveryReportSender reporter = mock(DqlRecoveryReportSender.class);
        doThrow(new IllegalStateException("runner unavailable"))
                .when(coordinator).start(org.mockito.ArgumentMatchers.any());
        DqlRecoveryMessageHandler handler = new DqlRecoveryMessageHandler(coordinator, reporter, contextProvider(), AGENT_ID);

        DqlRecoveryHandleResult failed = handler.handle(message());
        DqlRecoveryHandleResult retry = handler.handle(message());

        assertEquals(DqlRecoveryHandleResult.Outcome.REJECTED, failed.getOutcome());
        assertEquals(DqlRecoveryHandleResult.Outcome.DUPLICATE, retry.getOutcome());
        verify(coordinator).start(org.mockito.ArgumentMatchers.any());
        verify(reporter).reportBatchStarted(org.mockito.ArgumentMatchers.any());
        verify(reporter).reportBatchFailed(org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.eq("runner unavailable"),
                org.mockito.ArgumentMatchers.anyLong());
    }

    @Test
    @DisplayName("releases the batch claim when the initial callback fails")
    void callbackFailureReleasesBatchClaim() {
        DqlRecoveryCoordinator coordinator = mock(DqlRecoveryCoordinator.class);
        DqlRecoveryReportSender reporter = mock(DqlRecoveryReportSender.class);
        doThrow(new IllegalStateException("TM unavailable"))
                .when(reporter).reportBatchStarted(org.mockito.ArgumentMatchers.any());
        DqlRecoveryMessageHandler handler = new DqlRecoveryMessageHandler(coordinator, reporter, contextProvider(), AGENT_ID);

        DqlRecoveryHandleResult failed = handler.handle(message());
        DqlRecoveryHandleResult duplicate = handler.handle(message());

        assertEquals(DqlRecoveryHandleResult.Outcome.REJECTED, failed.getOutcome());
        assertEquals(DqlRecoveryHandleResult.Outcome.REJECTED, duplicate.getOutcome());
        verify(coordinator, never()).start(org.mockito.ArgumentMatchers.any());
        verify(reporter, org.mockito.Mockito.times(2))
                .reportBatchStarted(org.mockito.ArgumentMatchers.any());
        verify(reporter, never()).reportBatchFailed(org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyLong());
    }

    private DqlRecoveryTaskContextProvider contextProvider() {
        return taskId -> new DqlRecoveryTaskContext(taskId, 7L, AGENT_ID);
    }

    private Map<String, Object> message() {
        return new java.util.LinkedHashMap<>(Map.of(
                "type", "dqlRecovery",
                "taskId", TASK_ID,
                "batchId", BATCH_ID,
                "taskVersion", 7L,
                "orderedEventIds", List.of("event-2", "event-1"),
                "mode", "AUTO"
        ));
    }
}
