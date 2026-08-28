package com.tapdata.tm.dql.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dql.DqlEventStatusEnum;
import com.tapdata.tm.dql.DqlRecoveryCallbackResultEnum;
import com.tapdata.tm.dql.DqlRecoveryAttemptResultEnum;
import com.tapdata.tm.dql.DqlRecoveryBatchStatusEnum;
import com.tapdata.tm.dql.dto.DqlEventDto;
import com.tapdata.tm.dql.dto.DqlRecoveryAttemptDto;
import com.tapdata.tm.dql.dto.DqlRecoveryAuditEntryDto;
import com.tapdata.tm.dql.dto.DqlRecoveryBatchDto;
import com.tapdata.tm.dql.dto.DqlRecoveryMessageDto;
import com.tapdata.tm.dql.repository.DqlEventRepository;
import com.tapdata.tm.dql.repository.DqlRecoveryBatchRepository;
import com.tapdata.tm.dql.repository.DqlRecoveryTaskLockRepository;
import com.tapdata.tm.dql.vo.DqlRecoveryPreviewVo;
import com.tapdata.tm.dql.vo.DqlRecoveryRequestVo;
import com.tapdata.tm.dql.vo.DqlRecoveryResultReportVo;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.messagequeue.service.MessageQueueServiceImpl;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.service.WorkerService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DqlRecoveryBatchServiceTest {
    private static final String TASK_ID = "64f000000000000000000001";

    @Test
    @DisplayName("skipped event result increments skipped count without failure alarm")
    void skippedEventResultIncrementsSkippedCount() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlEventAlarmService alarmService = mock(DqlEventAlarmService.class);
        DqlRecoveryBatchService service = service(eventRepository, batchRepository, alarmService);
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1"));
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);
        when(eventRepository.failEventIdempotent(eq("DQL-1"), eq("DQLB-1"), any()))
                .thenReturn(DqlRecoveryCallbackResultEnum.APPLIED);

        DqlRecoveryResultReportVo report = report("DQLB-1", "DQL-1", "EVENT_RESULT");
        report.setResult(DqlRecoveryAttemptResultEnum.SKIPPED.name());

        service.report(TASK_ID, report);

        verify(batchRepository).increaseSkipped("DQLB-1");
        verify(batchRepository, never()).increaseFailed("DQLB-1");
        verify(alarmService, never()).notifyRecoveryFailed(any(DqlRecoveryBatchDto.class));
    }

    @Test
    @DisplayName("batch started moves a dispatched batch to running")
    void batchStartedMovesBatchToRunning() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryBatchService service = service(eventRepository, batchRepository, mock(DqlEventAlarmService.class));
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1"));
        batch.setStatus(DqlRecoveryBatchStatusEnum.DISPATCHED.name());
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);

        service.report(TASK_ID, report("DQLB-1", null, "BATCH_STARTED"));

        verify(batchRepository).markRunning("DQLB-1");
    }

    @Test
    @DisplayName("event started appends a running recovery attempt for the locked event")
    void eventStartedAppendsRunningAttempt() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryBatchService service = service(eventRepository, batchRepository, mock(DqlEventAlarmService.class));
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1"));
        batch.setStatus(DqlRecoveryBatchStatusEnum.RUNNING.name());
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);
        when(eventRepository.startEventIdempotent(eq("DQL-1"), eq("DQLB-1"), any(DqlRecoveryAttemptDto.class)))
                .thenReturn(DqlRecoveryCallbackResultEnum.APPLIED);
        DqlRecoveryResultReportVo report = report("DQLB-1", "DQL-1", "EVENT_STARTED");
        report.setStartedAt(1787580100000L);

        service.report(TASK_ID, report);

        ArgumentCaptor<DqlRecoveryAttemptDto> attempt = ArgumentCaptor.forClass(DqlRecoveryAttemptDto.class);
        verify(eventRepository).startEventIdempotent(eq("DQL-1"), eq("DQLB-1"), attempt.capture());
        assertEquals(DqlRecoveryAttemptResultEnum.RUNNING.name(), attempt.getValue().getResult());
        assertEquals(new java.util.Date(1787580100000L), attempt.getValue().getStartedAt());
        org.junit.jupiter.api.Assertions.assertNull(attempt.getValue().getFinishedAt());
    }

    @Test
    @DisplayName("event result keeps the Engine replay error message and stack")
    void eventResultKeepsReplayErrorDetails() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryBatchService service = service(eventRepository, batchRepository,
                mock(DqlEventAlarmService.class));
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1"));
        batch.setStatus(DqlRecoveryBatchStatusEnum.RUNNING.name());
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);
        when(eventRepository.failEventIdempotent(eq("DQL-1"), eq("DQLB-1"), any()))
                .thenReturn(DqlRecoveryCallbackResultEnum.APPLIED);
        DqlRecoveryResultReportVo report = report("DQLB-1", "DQL-1", "EVENT_RESULT");
        report.setResult(DqlRecoveryAttemptResultEnum.FAILED.name());
        report.setMessage("Duplicate entry '2' for key 'idx_unique_order_no'");
        report.setErrorDetails("java.sql.SQLIntegrityConstraintViolationException: Duplicate entry '2'");

        service.report(TASK_ID, report);

        ArgumentCaptor<DqlRecoveryAttemptDto> attempt = ArgumentCaptor.forClass(DqlRecoveryAttemptDto.class);
        verify(eventRepository).failEventIdempotent(eq("DQL-1"), eq("DQLB-1"), attempt.capture());
        assertEquals(report.getMessage(), attempt.getValue().getMessage());
        assertEquals(report.getErrorDetails(), attempt.getValue().getErrorDetails());
    }

    @Test
    @DisplayName("event result rejects an event that is not owned by the current batch")
    void eventResultRejectsEventOutsideCurrentBatchLock() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryBatchService service = service(eventRepository, batchRepository, mock(DqlEventAlarmService.class));
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1"));
        batch.setStatus(DqlRecoveryBatchStatusEnum.RUNNING.name());
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);
        when(eventRepository.completeEventIdempotent(eq("DQL-1"), eq("DQLB-1"), any()))
                .thenReturn(DqlRecoveryCallbackResultEnum.NOT_IN_BATCH);
        DqlRecoveryResultReportVo report = report("DQLB-1", "DQL-1", "EVENT_RESULT");
        report.setResult(DqlRecoveryAttemptResultEnum.SUCCESS.name());

        BizException exception = assertThrows(BizException.class, () -> service.report(TASK_ID, report));

        assertEquals("DqlRecovery.EventNotInBatch", exception.getErrorCode());
        verify(batchRepository, never()).increaseSuccess("DQLB-1");
    }

    @Test
    @DisplayName("batch finished requires event counters to reconcile before finalizing")
    void batchFinishedRequiresReconciledCounters() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryBatchService service = service(eventRepository, batchRepository, mock(DqlEventAlarmService.class));
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1", "DQL-2"));
        batch.setStatus(DqlRecoveryBatchStatusEnum.RUNNING.name());
        batch.setSuccessCount(1);
        batch.setFailedCount(0);
        batch.setSkippedCount(0);
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);

        BizException exception = assertThrows(BizException.class,
                () -> service.report(TASK_ID, report("DQLB-1", null, "BATCH_FINISHED")));

        assertEquals("DqlRecovery.CountMismatch", exception.getErrorCode());
        verify(batchRepository, never()).finish(anyString(), any(), any());
    }

    @Test
    @DisplayName("reconciled successful batch enters the success state")
    void batchFinishedFinalizesSuccessfulBatch() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlEventAlarmService alarmService = mock(DqlEventAlarmService.class);
        DqlRecoveryBatchService service = service(eventRepository, batchRepository, alarmService);
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1", "DQL-2"));
        batch.setSuccessCount(2);
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);

        service.report(TASK_ID, report("DQLB-1", null, "BATCH_FINISHED"));

        verify(batchRepository).finish("DQLB-1", DqlRecoveryBatchStatusEnum.SUCCESS, null);
        verify(alarmService, never()).notifyBatchPartialFailed(any(DqlRecoveryBatchDto.class));
    }

    @Test
    @DisplayName("reconciled partial batch raises one partial alarm and releases its task lock")
    void batchFinishedFinalizesPartialBatchAndReleasesTaskLock() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlEventAlarmService alarmService = mock(DqlEventAlarmService.class);
        DqlRecoveryTaskLockRepository taskLockRepository = mock(DqlRecoveryTaskLockRepository.class);
        DqlRecoveryBatchService service = lockedService(eventRepository, batchRepository, alarmService,
                taskLockRepository, mock(MessageQueueServiceImpl.class));
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1", "DQL-2"));
        batch.setSuccessCount(1);
        batch.setFailedCount(1);
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);

        service.report(TASK_ID, report("DQLB-1", null, "BATCH_FINISHED"));

        verify(batchRepository).finish("DQLB-1", DqlRecoveryBatchStatusEnum.PARTIAL_FAILED, null);
        verify(alarmService).notifyBatchPartialFailed(batch);
        verify(taskLockRepository).release(TASK_ID, "DQLB-1");
    }

    @Test
    @DisplayName("concurrent starts allow only the request that owns the task lock")
    void concurrentStartsAllowOnlyOneTaskLockOwner() throws Exception {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryTaskLockRepository taskLockRepository = mock(DqlRecoveryTaskLockRepository.class);
        DqlRecoveryBatchService service = lockedService(eventRepository, batchRepository,
                mock(DqlEventAlarmService.class), taskLockRepository, mock(MessageQueueServiceImpl.class));
        DqlEventDto event = event("DQL-1", TASK_ID, DqlEventStatusEnum.PENDING, "agent-1");
        AtomicBoolean lockTaken = new AtomicBoolean();
        when(eventRepository.findByEventIds(List.of("DQL-1"))).thenReturn(List.of(event));
        when(taskLockRepository.tryAcquire(eq(TASK_ID), anyString(), anyLong()))
                .thenAnswer(invocation -> lockTaken.compareAndSet(false, true));
        when(batchRepository.create(any(DqlRecoveryBatchDto.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(eventRepository.lockEvents(eq(List.of("DQL-1")), anyString())).thenReturn(1L);

        DqlRecoveryRequestVo request = request(List.of("DQL-1"));
        request.setConfirm(true);
        Callable<Object> start = () -> {
            try {
                return service.start(request, user());
            } catch (BizException exception) {
                return exception;
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            List<Future<Object>> results = executor.invokeAll(List.of(start, start));
            List<Object> completed = results.stream().map(this::getUnchecked).toList();

            assertEquals(1, completed.stream().filter(DqlRecoveryBatchDto.class::isInstance).count());
            List<BizException> conflicts = completed.stream()
                    .filter(BizException.class::isInstance)
                    .map(BizException.class::cast)
                    .toList();
            assertEquals(1, conflicts.size());
            assertEquals("DqlRecovery.BatchAlreadyRunning", conflicts.get(0).getErrorCode());
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    @DisplayName("duplicate event result does not increment counters or raise another alarm")
    void duplicateEventResultDoesNotChangeCounters() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlEventAlarmService alarmService = mock(DqlEventAlarmService.class);
        DqlRecoveryBatchService service = service(eventRepository, batchRepository, alarmService);
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1"));
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);
        when(eventRepository.completeEventIdempotent(eq("DQL-1"), eq("DQLB-1"), any()))
                .thenReturn(DqlRecoveryCallbackResultEnum.DUPLICATE);
        DqlRecoveryResultReportVo report = report("DQLB-1", "DQL-1", "EVENT_RESULT");
        report.setResult(DqlRecoveryAttemptResultEnum.SUCCESS.name());

        service.report(TASK_ID, report);

        verify(batchRepository, never()).increaseSuccess("DQLB-1");
        verify(alarmService, never()).notifyRecoveryFailed(any(DqlRecoveryBatchDto.class));
    }

    @Test
    @DisplayName("duplicate event result after batch completion is a no-op")
    void duplicateEventResultAfterBatchCompletionIsIdempotent() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryBatchService service = service(eventRepository, batchRepository, mock(DqlEventAlarmService.class));
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1"));
        batch.setStatus(DqlRecoveryBatchStatusEnum.SUCCESS.name());
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);
        DqlEventDto event = new DqlEventDto();
        DqlRecoveryAttemptDto attempt = new DqlRecoveryAttemptDto();
        attempt.setBatchId("DQLB-1");
        attempt.setAttemptId("A-1");
        attempt.setResult(DqlRecoveryAttemptResultEnum.SUCCESS.name());
        event.setRecoveryAttempts(List.of(attempt));
        when(eventRepository.findByEventId("DQL-1")).thenReturn(event);
        DqlRecoveryResultReportVo report = report("DQLB-1", "DQL-1", "EVENT_RESULT");
        report.setResult(DqlRecoveryAttemptResultEnum.SUCCESS.name());

        service.report(TASK_ID, report);

        verify(eventRepository).findByEventId("DQL-1");
        verify(eventRepository, never()).completeEventIdempotent(eq("DQL-1"), eq("DQLB-1"), any());
        verify(batchRepository, never()).increaseSuccess("DQLB-1");
    }

    @Test
    @DisplayName("conflicting event result is rejected for the same attempt")
    void conflictingEventResultIsRejected() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryBatchService service = service(eventRepository, batchRepository, mock(DqlEventAlarmService.class));
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1"));
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);
        when(eventRepository.completeEventIdempotent(eq("DQL-1"), eq("DQLB-1"), any()))
                .thenReturn(DqlRecoveryCallbackResultEnum.CONFLICT);
        DqlRecoveryResultReportVo report = report("DQLB-1", "DQL-1", "EVENT_RESULT");
        report.setResult(DqlRecoveryAttemptResultEnum.SUCCESS.name());

        BizException exception = assertThrows(BizException.class, () -> service.report(TASK_ID, report));

        assertEquals("DqlRecovery.AttemptConflict", exception.getErrorCode());
        verify(batchRepository, never()).increaseSuccess("DQLB-1");
    }

    @Test
    @DisplayName("repeated batch started callback is a no-op after the batch is running")
    void repeatedBatchStartedIsIdempotent() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryBatchService service = service(eventRepository, batchRepository, mock(DqlEventAlarmService.class));
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1"));
        batch.setStatus(DqlRecoveryBatchStatusEnum.RUNNING.name());
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);

        service.report(TASK_ID, report("DQLB-1", null, "BATCH_STARTED"));

        verify(batchRepository, never()).markRunning("DQLB-1");
    }

    @Test
    @DisplayName("heartbeat refreshes a running recovery batch")
    void heartbeatRefreshesRunningBatch() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryBatchService service = service(eventRepository, batchRepository, mock(DqlEventAlarmService.class));
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1"));
        batch.setStatus(DqlRecoveryBatchStatusEnum.RUNNING.name());
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);

        service.report(TASK_ID, report("DQLB-1", null, "BATCH_HEARTBEAT"));

        verify(batchRepository).touchHeartbeat(eq("DQLB-1"), any(java.util.Date.class));
    }

    @Test
    @DisplayName("late heartbeat is ignored after the recovery batch reaches a terminal state")
    void lateHeartbeatIsIgnoredAfterTerminalBatch() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryBatchService service = service(eventRepository, batchRepository, mock(DqlEventAlarmService.class));
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1"));
        batch.setStatus(DqlRecoveryBatchStatusEnum.SUCCESS.name());
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);

        service.report(TASK_ID, report("DQLB-1", null, "BATCH_HEARTBEAT"));

        verify(batchRepository, never()).touchHeartbeat(anyString(), any(java.util.Date.class));
    }

    @Test
    @DisplayName("repeated finished callback is a no-op after terminal success")
    void repeatedFinishedCallbackIsIdempotent() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryBatchService service = service(eventRepository, batchRepository, mock(DqlEventAlarmService.class));
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1"));
        batch.setStatus(DqlRecoveryBatchStatusEnum.SUCCESS.name());
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);

        service.report(TASK_ID, report("DQLB-1", null, "BATCH_FINISHED"));

        verify(batchRepository, never()).finish(anyString(), any(), any());
    }

    @Test
    @DisplayName("repeated failed callback is a no-op after terminal failure")
    void repeatedFailedCallbackIsIdempotent() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlEventAlarmService alarmService = mock(DqlEventAlarmService.class);
        DqlRecoveryBatchService service = service(eventRepository, batchRepository, alarmService);
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1"));
        batch.setStatus(DqlRecoveryBatchStatusEnum.FAILED.name());
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);

        service.report(TASK_ID, report("DQLB-1", null, "BATCH_FAILED"));

        verify(eventRepository, never()).releaseBatchLocks(anyString(), any(DqlEventStatusEnum.class));
        verify(batchRepository, never()).finish(anyString(), any(), any());
        verify(alarmService, never()).notifyRecoveryFailed(any(DqlRecoveryBatchDto.class));
    }

    @Test
    @DisplayName("timeout scan marks all unresolved events failed and releases the task lock")
    void timeoutScanFailsAllUnresolvedEvents() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlEventAlarmService alarmService = mock(DqlEventAlarmService.class);
        DqlRecoveryTaskLockRepository taskLockRepository = mock(DqlRecoveryTaskLockRepository.class);
        DqlRecoveryBatchService service = lockedService(eventRepository, batchRepository, alarmService,
                taskLockRepository, mock(MessageQueueServiceImpl.class));
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1", "DQL-2"));
        batch.setStatus(DqlRecoveryBatchStatusEnum.RUNNING.name());
        when(batchRepository.findTimedOut(any(java.util.Date.class), any(java.util.Date.class), any(java.util.Date.class)))
                .thenReturn(List.of(batch));
        when(eventRepository.timeoutEvents(eq("DQLB-1"), eq(List.of("DQL-1", "DQL-2")), any()))
                .thenReturn(2L);
        when(eventRepository.countReprocessingByBatchId("DQLB-1")).thenReturn(0L);
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);
        when(batchRepository.finishTimedOut(eq("DQLB-1"), eq(DqlRecoveryBatchStatusEnum.FAILED),
                eq("Recovery batch timed out"), any(java.util.Date.class), any(java.util.Date.class),
                any(java.util.Date.class))).thenReturn(true);

        assertEquals(1, service.timeoutExpiredBatches(new java.util.Date(1787582000000L)));

        verify(batchRepository).increaseFailed("DQLB-1", 2);
        verify(batchRepository).finishTimedOut(eq("DQLB-1"), eq(DqlRecoveryBatchStatusEnum.FAILED),
                eq("Recovery batch timed out"), any(java.util.Date.class), any(java.util.Date.class),
                any(java.util.Date.class));
        verify(alarmService).notifyRecoveryFailed(batch);
        verify(taskLockRepository).release(TASK_ID, "DQLB-1");
    }

    @Test
    @DisplayName("timeout scan summarizes a partially completed batch as partial failure")
    void timeoutScanSummarizesPartialFailure() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlEventAlarmService alarmService = mock(DqlEventAlarmService.class);
        DqlRecoveryBatchService service = lockedService(eventRepository, batchRepository, alarmService,
                mock(DqlRecoveryTaskLockRepository.class), mock(MessageQueueServiceImpl.class));
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1", "DQL-2"));
        batch.setSuccessCount(1);
        when(batchRepository.findTimedOut(any(java.util.Date.class), any(java.util.Date.class), any(java.util.Date.class)))
                .thenReturn(List.of(batch));
        when(eventRepository.timeoutEvents(eq("DQLB-1"), eq(List.of("DQL-1", "DQL-2")), any()))
                .thenReturn(1L);
        when(eventRepository.countReprocessingByBatchId("DQLB-1")).thenReturn(0L);
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);
        when(batchRepository.finishTimedOut(eq("DQLB-1"), eq(DqlRecoveryBatchStatusEnum.PARTIAL_FAILED),
                eq("Recovery batch timed out"), any(java.util.Date.class), any(java.util.Date.class),
                any(java.util.Date.class))).thenReturn(true);

        assertEquals(1, service.timeoutExpiredBatches(new java.util.Date(1787582000000L)));

        verify(batchRepository).finishTimedOut(eq("DQLB-1"), eq(DqlRecoveryBatchStatusEnum.PARTIAL_FAILED),
                eq("Recovery batch timed out"), any(java.util.Date.class), any(java.util.Date.class),
                any(java.util.Date.class));
        verify(alarmService).notifyRecoveryFailed(batch);
    }

    @Test
    @DisplayName("timeout scan leaves the batch active while events are still owned by it")
    void timeoutScanWaitsForRemainingReprocessingEvents() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlEventAlarmService alarmService = mock(DqlEventAlarmService.class);
        DqlRecoveryTaskLockRepository taskLockRepository = mock(DqlRecoveryTaskLockRepository.class);
        DqlRecoveryBatchService service = lockedService(eventRepository, batchRepository, alarmService,
                taskLockRepository, mock(MessageQueueServiceImpl.class));
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1"));
        when(batchRepository.findTimedOut(any(java.util.Date.class), any(java.util.Date.class), any(java.util.Date.class)))
                .thenReturn(List.of(batch));
        when(eventRepository.timeoutEvents(eq("DQLB-1"), eq(List.of("DQL-1")), any())).thenReturn(0L);
        when(eventRepository.countReprocessingByBatchId("DQLB-1")).thenReturn(1L);

        assertEquals(0, service.timeoutExpiredBatches(new java.util.Date(1787582000000L)));

        verify(batchRepository, never()).finishTimedOut(anyString(), any(), anyString(),
                any(java.util.Date.class), any(java.util.Date.class), any(java.util.Date.class));
        verify(taskLockRepository, never()).release(anyString(), anyString());
        verify(alarmService, never()).notifyRecoveryFailed(any(DqlRecoveryBatchDto.class));
    }

    @Test
    @DisplayName("dispatch timeout reports that Engine never accepted the recovery batch")
    void dispatchTimeoutReportsEngineNotAccepted() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlEventAlarmService alarmService = mock(DqlEventAlarmService.class);
        DqlRecoveryTaskLockRepository taskLockRepository = mock(DqlRecoveryTaskLockRepository.class);
        DqlRecoveryBatchService service = lockedService(eventRepository, batchRepository, alarmService,
                taskLockRepository, mock(MessageQueueServiceImpl.class));
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1"));
        batch.setStatus(DqlRecoveryBatchStatusEnum.DISPATCHED.name());
        when(batchRepository.findTimedOut(any(java.util.Date.class), any(java.util.Date.class), any(java.util.Date.class)))
                .thenReturn(List.of(batch));
        when(eventRepository.timeoutEvents(eq("DQLB-1"), eq(List.of("DQL-1")), any())).thenReturn(1L);
        when(eventRepository.countReprocessingByBatchId("DQLB-1")).thenReturn(0L);
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);
        when(batchRepository.finishTimedOut(eq("DQLB-1"), eq(DqlRecoveryBatchStatusEnum.FAILED),
                eq("DQL reprocessing timed out before Engine accepted the recovery batch"),
                any(java.util.Date.class), any(java.util.Date.class), any(java.util.Date.class))).thenReturn(true);

        assertEquals(1, service.timeoutExpiredBatches(new java.util.Date(1787582000000L)));

        verify(batchRepository).finishTimedOut(eq("DQLB-1"), eq(DqlRecoveryBatchStatusEnum.FAILED),
                eq("DQL reprocessing timed out before Engine accepted the recovery batch"),
                any(java.util.Date.class), any(java.util.Date.class), any(java.util.Date.class));
    }

    @Test
    @DisplayName("heartbeat timeout reports that Engine recovery execution went stale")
    void heartbeatTimeoutReportsEngineExecutionStale() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlEventAlarmService alarmService = mock(DqlEventAlarmService.class);
        DqlRecoveryTaskLockRepository taskLockRepository = mock(DqlRecoveryTaskLockRepository.class);
        DqlRecoveryBatchService service = lockedService(eventRepository, batchRepository, alarmService,
                taskLockRepository, mock(MessageQueueServiceImpl.class));
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1"));
        batch.setPingTime(new java.util.Date(1787580000000L));
        when(batchRepository.findTimedOut(any(java.util.Date.class), any(java.util.Date.class), any(java.util.Date.class)))
                .thenReturn(List.of(batch));
        when(eventRepository.timeoutEvents(eq("DQLB-1"), eq(List.of("DQL-1")), any())).thenReturn(1L);
        when(eventRepository.countReprocessingByBatchId("DQLB-1")).thenReturn(0L);
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);
        when(batchRepository.finishTimedOut(eq("DQLB-1"), eq(DqlRecoveryBatchStatusEnum.FAILED),
                eq("DQL reprocessing timed out because the Engine heartbeat expired"),
                any(java.util.Date.class), any(java.util.Date.class), any(java.util.Date.class))).thenReturn(true);

        assertEquals(1, service.timeoutExpiredBatches(new java.util.Date(1787582000000L)));

        verify(batchRepository).finishTimedOut(eq("DQLB-1"), eq(DqlRecoveryBatchStatusEnum.FAILED),
                eq("DQL reprocessing timed out because the Engine heartbeat expired"),
                any(java.util.Date.class), any(java.util.Date.class), any(java.util.Date.class));
    }

    @Test
    @DisplayName("timeout scan does not finalize a batch refreshed after it was selected")
    void timeoutScanSkipsBatchRefreshedAfterSelection() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlEventAlarmService alarmService = mock(DqlEventAlarmService.class);
        DqlRecoveryBatchService service = lockedService(eventRepository, batchRepository, alarmService,
                mock(DqlRecoveryTaskLockRepository.class), mock(MessageQueueServiceImpl.class));
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1"));
        batch.setStatus(DqlRecoveryBatchStatusEnum.RUNNING.name());
        batch.setPingTime(new java.util.Date(1787581999000L));
        when(batchRepository.findTimedOut(any(java.util.Date.class), any(java.util.Date.class), any(java.util.Date.class)))
                .thenReturn(List.of(batch));
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);

        assertEquals(0, service.timeoutExpiredBatches(new java.util.Date(1787582000000L)));

        verify(eventRepository, never()).timeoutEvents(anyString(), anyList(), any());
        verify(batchRepository, never()).finishTimedOut(anyString(), any(), anyString(),
                any(java.util.Date.class), any(java.util.Date.class), any(java.util.Date.class));
        verify(alarmService, never()).notifyRecoveryFailed(any(DqlRecoveryBatchDto.class));
    }

    @Test
    @DisplayName("blocked preview events expose the frontend message and identifying context")
    void blockedPreviewEventsExposeFrontendContract() throws Exception {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchService service = service(eventRepository, mock(DqlRecoveryBatchRepository.class), mock(DqlEventAlarmService.class));
        DqlEventDto blocked = event("DQL-1", TASK_ID, DqlEventStatusEnum.RECOVERED, "agent-1");
        blocked.setSourceTable("orders");
        blocked.setTargetTable("orders_sink");
        blocked.setDmlType("U");
        blocked.setEventTime(new java.util.Date(1000L));
        blocked.setCaptureSeq(7L);
        when(eventRepository.findByEventIds(List.of("DQL-1"))).thenReturn(List.of(blocked));

        DqlRecoveryPreviewVo preview = service.preview(request(List.of("DQL-1")), user());
        String json = new ObjectMapper().writeValueAsString(preview);

        assertTrue(json.contains("\"blockedEvents\":[{\"eventId\":\"DQL-1\",\"message\""));
        assertFalse(json.contains("\"reason\""));
        assertTrue(json.contains("\"sourceTable\":\"orders\""));
        assertTrue(json.contains("\"targetTable\":\"orders_sink\""));
        assertTrue(json.contains("\"dmlType\":\"U\""));
        assertTrue(json.contains("\"captureSeq\":7"));
    }

    @Test
    @DisplayName("preview accepts a recoverable event when its task and agent are ready")
    void previewAcceptsEventWhenTaskAndAgentAreReady() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        TaskService taskService = mock(TaskService.class);
        WorkerService workerService = mock(WorkerService.class);
        DqlRecoveryBatchService service = strictService(eventRepository, batchRepository, taskService, workerService);
        DqlEventDto event = recoveryEvent("DQL-1", 7L);
        event.setTargetTable("orders_sink");
        event.setErrorType("TARGET_WRITE_ERROR");
        event.setErrorCode("TARGET_WRITE_FAILED");
        event.setFailedAt(new java.util.Date(1500L));
        event.setRecoveryCount(2);
        event.setLastRecoveryTime(new java.util.Date(2000L));
        TaskDto task = recoveryTask("running", 7L, "agent-1");
        WorkerDto worker = recoveryWorker("agent-1");
        when(eventRepository.findByEventIds(List.of("DQL-1"))).thenReturn(List.of(event));
        when(taskService.findByTaskId(eq(new ObjectId(TASK_ID)), eq("name"), eq("status"), eq("version"), eq("agentId")))
                .thenReturn(task);
        when(workerService.queryWorkerByProcessId("agent-1")).thenReturn(worker);
        when(workerService.isAgentTimeout(worker.getPingTime())).thenReturn(false);
        when(batchRepository.findActiveByTaskId(TASK_ID)).thenReturn(null);

        DqlRecoveryPreviewVo preview = service.preview(request(List.of("DQL-1")), user());

        assertTrue(preview.isCanSubmit());
        assertEquals(List.of("DQL-1"), preview.getOrderedEvents().stream()
                .map(DqlRecoveryPreviewVo.OrderedEvent::getEventId).toList());
        DqlRecoveryPreviewVo.OrderedEvent ordered = preview.getOrderedEvents().get(0);
        assertEquals("orders_sink", ordered.getTargetTable());
        assertEquals("TARGET_WRITE_ERROR", ordered.getErrorType());
        assertEquals("TARGET_WRITE_FAILED", ordered.getErrorCode());
        assertEquals(new java.util.Date(1500L), ordered.getFailedAt());
        assertEquals(2, ordered.getRecoveryCount());
        assertEquals(new java.util.Date(2000L), ordered.getLastRecoveryTime());
        assertTrue(preview.getBlockedEvents().isEmpty());
    }

    @Test
    @DisplayName("preview blocks an event when its business key is missing")
    void previewBlocksMissingBusinessKey() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        TaskService taskService = mock(TaskService.class);
        WorkerService workerService = mock(WorkerService.class);
        DqlRecoveryBatchService service = strictService(eventRepository, batchRepository, taskService, workerService);
        DqlEventDto event = recoveryEvent("DQL-1", 7L);
        event.setEventKeyMissing(true);
        event.setRecordIdentity(null);
        when(eventRepository.findByEventIds(List.of("DQL-1"))).thenReturn(List.of(event));
        prepareReadyTask(taskService, workerService);
        when(batchRepository.findActiveByTaskId(TASK_ID)).thenReturn(null);

        DqlRecoveryPreviewVo preview = service.preview(request(List.of("DQL-1")), user());

        assertFalse(preview.isCanSubmit());
        assertTrue(preview.getOrderedEvents().isEmpty());
        assertEquals("event has no business key", preview.getBlockedEvents().get(0).getMessage());
    }

    @Test
    @DisplayName("preview blocks an event when the current task version has changed")
    void previewBlocksTaskVersionMismatch() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        TaskService taskService = mock(TaskService.class);
        WorkerService workerService = mock(WorkerService.class);
        DqlRecoveryBatchService service = strictService(eventRepository, batchRepository, taskService, workerService);
        DqlEventDto event = recoveryEvent("DQL-1", 7L);
        when(eventRepository.findByEventIds(List.of("DQL-1"))).thenReturn(List.of(event));
        when(taskService.findByTaskId(eq(new ObjectId(TASK_ID)), eq("name"), eq("status"), eq("version"), eq("agentId")))
                .thenReturn(recoveryTask("running", 8L, "agent-1"));
        prepareReadyWorker(workerService);
        when(batchRepository.findActiveByTaskId(TASK_ID)).thenReturn(null);

        DqlRecoveryPreviewVo preview = service.preview(request(List.of("DQL-1")), user());

        assertFalse(preview.isCanSubmit());
        assertEquals("task version has changed", preview.getBlockedEvents().get(0).getMessage());
    }

    @Test
    @DisplayName("preview identifies an unavailable current task version")
    void previewBlocksUnavailableCurrentTaskVersion() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        TaskService taskService = mock(TaskService.class);
        WorkerService workerService = mock(WorkerService.class);
        DqlRecoveryBatchService service = strictService(eventRepository, batchRepository, taskService, workerService);
        DqlEventDto event = recoveryEvent("DQL-1", 7L);
        TaskDto task = recoveryTask("running", 7L, "agent-1");
        task.setVersion(null);
        when(eventRepository.findByEventIds(List.of("DQL-1"))).thenReturn(List.of(event));
        when(taskService.findByTaskId(eq(new ObjectId(TASK_ID)), eq("name"), eq("status"), eq("version"), eq("agentId")))
                .thenReturn(task);
        prepareReadyWorker(workerService);
        when(batchRepository.findActiveByTaskId(TASK_ID)).thenReturn(null);

        DqlRecoveryPreviewVo preview = service.preview(request(List.of("DQL-1")), user());

        assertFalse(preview.isCanSubmit());
        assertEquals("current task version is unavailable", preview.getBlockedEvents().get(0).getMessage());
    }

    @Test
    @DisplayName("preview identifies an unavailable event task version")
    void previewBlocksUnavailableEventTaskVersion() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        TaskService taskService = mock(TaskService.class);
        WorkerService workerService = mock(WorkerService.class);
        DqlRecoveryBatchService service = strictService(eventRepository, batchRepository, taskService, workerService);
        DqlEventDto event = recoveryEvent("DQL-1", 7L);
        event.setTaskVersion(null);
        when(eventRepository.findByEventIds(List.of("DQL-1"))).thenReturn(List.of(event));
        when(taskService.findByTaskId(eq(new ObjectId(TASK_ID)), eq("name"), eq("status"), eq("version"), eq("agentId")))
                .thenReturn(recoveryTask("running", 7L, "agent-1"));
        prepareReadyWorker(workerService);
        when(batchRepository.findActiveByTaskId(TASK_ID)).thenReturn(null);

        DqlRecoveryPreviewVo preview = service.preview(request(List.of("DQL-1")), user());

        assertFalse(preview.isCanSubmit());
        assertEquals("event task version is unavailable", preview.getBlockedEvents().get(0).getMessage());
    }

    @Test
    @DisplayName("preview allows an event when the task is not running")
    void previewAllowsNonRunningTaskStatus() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        TaskService taskService = mock(TaskService.class);
        WorkerService workerService = mock(WorkerService.class);
        DqlRecoveryBatchService service = strictService(eventRepository, batchRepository, taskService, workerService);
        when(eventRepository.findByEventIds(List.of("DQL-1"))).thenReturn(List.of(recoveryEvent("DQL-1", 7L)));
        when(taskService.findByTaskId(eq(new ObjectId(TASK_ID)), eq("name"), eq("status"), eq("version"), eq("agentId")))
                .thenReturn(recoveryTask(TaskDto.STATUS_EDIT, 7L, "agent-1"));
        prepareReadyWorker(workerService);
        when(batchRepository.findActiveByTaskId(TASK_ID)).thenReturn(null);

        DqlRecoveryPreviewVo preview = service.preview(request(List.of("DQL-1")), user());

        assertTrue(preview.isCanSubmit());
        assertEquals(List.of("DQL-1"), preview.getOrderedEvents().stream()
                .map(DqlRecoveryPreviewVo.OrderedEvent::getEventId)
                .toList());
    }

    @Test
    @DisplayName("preview rejects a selected event when the task no longer exists")
    void previewRejectsMissingTask() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        TaskService taskService = mock(TaskService.class);
        WorkerService workerService = mock(WorkerService.class);
        DqlRecoveryBatchService service = strictService(eventRepository, batchRepository, taskService, workerService);
        when(eventRepository.findByEventIds(List.of("DQL-1"))).thenReturn(List.of(recoveryEvent("DQL-1", 7L)));
        when(taskService.findByTaskId(eq(new ObjectId(TASK_ID)), eq("name"), eq("status"), eq("version"), eq("agentId")))
                .thenReturn(null);

        BizException exception = assertThrows(BizException.class,
                () -> service.preview(request(List.of("DQL-1")), user()));

        assertEquals("Task.NotFound", exception.getErrorCode());
    }

    @Test
    @DisplayName("preview blocks all selected events when the assigned agent is unavailable")
    void previewBlocksUnavailableAgent() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        TaskService taskService = mock(TaskService.class);
        WorkerService workerService = mock(WorkerService.class);
        DqlRecoveryBatchService service = strictService(eventRepository, batchRepository, taskService, workerService);
        when(eventRepository.findByEventIds(List.of("DQL-1"))).thenReturn(List.of(recoveryEvent("DQL-1", 7L)));
        when(taskService.findByTaskId(eq(new ObjectId(TASK_ID)), eq("name"), eq("status"), eq("version"), eq("agentId")))
                .thenReturn(recoveryTask("running", 7L, "agent-1"));
        when(workerService.queryWorkerByProcessId("agent-1")).thenReturn(null);
        when(batchRepository.findActiveByTaskId(TASK_ID)).thenReturn(null);

        DqlRecoveryPreviewVo preview = service.preview(request(List.of("DQL-1")), user());

        assertFalse(preview.isCanSubmit());
        assertEquals("agent is not available", preview.getBlockedEvents().get(0).getMessage());
    }

    @Test
    @DisplayName("preview blocks a task with an existing active recovery batch")
    void previewBlocksActiveBatch() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        TaskService taskService = mock(TaskService.class);
        WorkerService workerService = mock(WorkerService.class);
        DqlRecoveryBatchService service = strictService(eventRepository, batchRepository, taskService, workerService);
        when(eventRepository.findByEventIds(List.of("DQL-1"))).thenReturn(List.of(recoveryEvent("DQL-1", 7L)));
        prepareReadyTask(taskService, workerService);
        DqlRecoveryBatchDto activeBatch = batch("DQLB-active", List.of("DQL-old"));
        activeBatch.setStatus(DqlRecoveryBatchStatusEnum.RUNNING.name());
        when(batchRepository.findActiveByTaskId(TASK_ID)).thenReturn(activeBatch);

        DqlRecoveryPreviewVo preview = service.preview(request(List.of("DQL-1")), user());

        assertFalse(preview.isCanSubmit());
        assertEquals("an active recovery batch already exists", preview.getBlockedEvents().get(0).getMessage());
    }

    @Test
    @DisplayName("start uses the task lock as the authority when a stale active batch record remains")
    void startCanRecoverWhenActiveBatchRecordIsStale() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryTaskLockRepository taskLockRepository = mock(DqlRecoveryTaskLockRepository.class);
        DqlRecoveryBatchService service = lockedService(eventRepository, batchRepository,
                mock(DqlEventAlarmService.class), taskLockRepository, mock(MessageQueueServiceImpl.class));
        when(eventRepository.findByEventIds(List.of("DQL-1")))
                .thenReturn(List.of(event("DQL-1", TASK_ID, DqlEventStatusEnum.PENDING, "agent-1")));
        DqlRecoveryBatchDto staleBatch = batch("DQLB-stale", List.of("DQL-old"));
        staleBatch.setStatus(DqlRecoveryBatchStatusEnum.RUNNING.name());
        when(batchRepository.findActiveByTaskId(TASK_ID)).thenReturn(staleBatch);
        when(taskLockRepository.tryAcquire(eq(TASK_ID), anyString(), anyLong())).thenReturn(true);
        when(batchRepository.create(any(DqlRecoveryBatchDto.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(eventRepository.lockEvents(eq(List.of("DQL-1")), anyString())).thenReturn(1L);
        DqlRecoveryRequestVo request = request(List.of("DQL-1"));
        request.setConfirm(true);

        DqlRecoveryBatchDto result = service.start(request, user());

        assertEquals(DqlRecoveryBatchStatusEnum.DISPATCHED.name(), result.getStatus());
        verify(taskLockRepository).tryAcquire(eq(TASK_ID), anyString(), anyLong());
    }

    @Test
    @DisplayName("preview blocks every selected event when the batch exceeds the default limit")
    void previewBlocksBatchOverLimit() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        TaskService taskService = mock(TaskService.class);
        WorkerService workerService = mock(WorkerService.class);
        DqlRecoveryBatchService service = strictService(eventRepository, batchRepository, taskService, workerService);
        List<String> eventIds = java.util.stream.IntStream.range(0, 201)
                .mapToObj(index -> "DQL-" + index).toList();
        List<DqlEventDto> events = java.util.stream.IntStream.range(0, 201)
                .mapToObj(index -> recoveryEvent("DQL-" + index, 7L)).toList();
        when(eventRepository.findByEventIds(anyList())).thenReturn(events);
        prepareReadyTask(taskService, workerService);
        when(batchRepository.findActiveByTaskId(TASK_ID)).thenReturn(null);

        DqlRecoveryPreviewVo preview = service.preview(request(eventIds), user());

        assertFalse(preview.isCanSubmit());
        assertTrue(preview.getOrderedEvents().isEmpty());
        assertEquals(201, preview.getBlockedEvents().size());
        assertEquals("recovery batch size exceeds 200", preview.getBlockedEvents().get(0).getMessage());
    }

    @Test
    @DisplayName("start stores the current task status and version in the recovery batch")
    void startStoresCurrentTaskContext() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        TaskService taskService = mock(TaskService.class);
        WorkerService workerService = mock(WorkerService.class);
        MessageQueueServiceImpl messageQueueService = mock(MessageQueueServiceImpl.class);
        DqlRecoveryBatchService service = new DqlRecoveryBatchService(
                eventRepository,
                batchRepository,
                mock(DqlEventPermissionService.class),
                messageQueueService,
                mock(DqlEventAlarmService.class),
                taskService,
                workerService
        );
        DqlEventDto event = recoveryEvent("DQL-1", 7L);
        when(eventRepository.findByEventIds(List.of("DQL-1"))).thenReturn(List.of(event));
        prepareReadyTask(taskService, workerService);
        when(batchRepository.findActiveByTaskId(TASK_ID)).thenReturn(null);
        when(batchRepository.create(any(DqlRecoveryBatchDto.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(eventRepository.lockEvents(eq(List.of("DQL-1")), anyString())).thenReturn(1L);
        DqlRecoveryRequestVo request = request(List.of("DQL-1"));
        request.setConfirm(true);

        DqlRecoveryBatchDto result = service.start(request, user());

        assertEquals("running", result.getTaskStatusBefore());
        assertEquals("running", result.getTaskStatusAfter());
        assertEquals(DqlRecoveryMessageDto.MODE_AUTO, result.getMode());
        assertEquals(7L, result.getTaskVersion());
        assertEquals("agent-1", result.getAgentId());
        assertTrue(result.getAuditEntries().stream().anyMatch(entry -> "BATCH_CREATED".equals(entry.getType())));
    }

    @Test
    @DisplayName("source read result is stored for batch detail and audit")
    void sourceReadResultIsStoredForBatchDetailAndAudit() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryBatchService service = service(eventRepository, batchRepository, mock(DqlEventAlarmService.class));

        service.recordSourceReadResult("DQLB-1", true, "SUCCESS", "source reads paused", 1787580100000L);

        ArgumentCaptor<DqlRecoveryAuditEntryDto> audit = ArgumentCaptor.forClass(DqlRecoveryAuditEntryDto.class);
        verify(batchRepository).recordSourceReadResult(eq("DQLB-1"), eq(true), eq("SUCCESS"),
                eq("source reads paused"), eq(new java.util.Date(1787580100000L)), audit.capture());
        assertEquals("SOURCE_READ_PAUSE", audit.getValue().getType());
        assertEquals("SUCCESS", audit.getValue().getStatus());
    }

    @Test
    @DisplayName("start rejects an unsupported recovery mode")
    void startRejectsUnsupportedMode() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryBatchService service = service(eventRepository, batchRepository, mock(DqlEventAlarmService.class));
        DqlRecoveryRequestVo request = request(List.of("DQL-1"));
        request.setConfirm(true);
        request.setMode("MANUAL");

        BizException exception = assertThrows(BizException.class, () -> service.start(request, user()));

        assertEquals("IllegalArgument", exception.getErrorCode());
        verify(batchRepository, never()).create(any(DqlRecoveryBatchDto.class));
    }

    @Test
    @DisplayName("start dispatches the server ordered event ids and marks the batch dispatched")
    void startDispatchesOrderedEvents() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        MessageQueueServiceImpl messageQueueService = mock(MessageQueueServiceImpl.class);
        DqlEventPermissionService permissionService = mock(DqlEventPermissionService.class);
        DqlRecoveryBatchService service = new DqlRecoveryBatchService(
                eventRepository, batchRepository, permissionService, messageQueueService, mock(DqlEventAlarmService.class));
        DqlEventDto event = event("DQL-1", TASK_ID, DqlEventStatusEnum.PENDING, "agent-1");
        event.setTaskVersion(8L);
        when(eventRepository.findByEventIds(List.of("DQL-1"))).thenReturn(List.of(event));
        when(batchRepository.create(any(DqlRecoveryBatchDto.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(eventRepository.lockEvents(eq(List.of("DQL-1")), anyString())).thenReturn(1L);
        DqlRecoveryRequestVo request = request(List.of("DQL-1"));
        request.setConfirm(true);

        DqlRecoveryBatchDto result = service.start(request, user());

        assertEquals(DqlRecoveryBatchStatusEnum.DISPATCHED.name(), result.getStatus());
        ArgumentCaptor<Map<String, Object>> payload = ArgumentCaptor.forClass(Map.class);
        verify(messageQueueService).sendPipeMessage(payload.capture(), eq("tm"), eq("agent-1"));
        assertEquals(DqlRecoveryMessageDto.TYPE, payload.getValue().get("type"));
        assertEquals(TASK_ID, payload.getValue().get("taskId"));
        assertEquals(8L, payload.getValue().get("taskVersion"));
        assertEquals(List.of("DQL-1"), payload.getValue().get("orderedEventIds"));
        assertEquals("user-id", payload.getValue().get("operatorId"));
        assertEquals("Harsen", payload.getValue().get("operatorName"));
        assertEquals(DqlRecoveryMessageDto.MODE_AUTO, payload.getValue().get("mode"));
        assertFalse(payload.getValue().containsKey("opType"));
        InOrder order = inOrder(batchRepository, messageQueueService);
        order.verify(batchRepository).updateStatus(anyString(), eq(DqlRecoveryBatchStatusEnum.DISPATCHED), eq(null));
        order.verify(messageQueueService).sendPipeMessage(any(), eq("tm"), eq("agent-1"));
    }

    @Test
    @DisplayName("preview, batch and dispatch share the stable recovery order")
    void persistsAndDispatchesStableRecoveryOrder() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        MessageQueueServiceImpl messageQueueService = mock(MessageQueueServiceImpl.class);
        DqlRecoveryBatchService service = new DqlRecoveryBatchService(
                eventRepository,
                batchRepository,
                mock(DqlEventPermissionService.class),
                messageQueueService,
                mock(DqlEventAlarmService.class));
        DqlEventDto first = event("DQL-1", TASK_ID, DqlEventStatusEnum.PENDING, "agent-1");
        first.setSourceTable("orders");
        first.setTargetTable("orders_sink");
        first.setEventTime(new java.util.Date(1000L));
        first.setCaptureSeq(1L);
        DqlEventDto second = event("DQL-2", TASK_ID, DqlEventStatusEnum.PENDING, "agent-1");
        second.setSourceTable("customers");
        second.setTargetTable("customers_sink");
        second.setEventTime(new java.util.Date(1000L));
        second.setCaptureSeq(1L);
        DqlEventDto third = event("DQL-3", TASK_ID, DqlEventStatusEnum.PENDING, "agent-1");
        third.setSourceTable("products");
        third.setTargetTable("products_sink");
        third.setEventTime(new java.util.Date(1000L));
        third.setCaptureSeq(2L);
        when(eventRepository.findByEventIds(anyList())).thenReturn(List.of(third, first, second));
        when(batchRepository.create(any(DqlRecoveryBatchDto.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(eventRepository.lockEvents(eq(List.of("DQL-1", "DQL-2", "DQL-3")), anyString())).thenReturn(3L);
        DqlRecoveryRequestVo request = request(List.of("DQL-3", "DQL-2", "DQL-1"));
        request.setConfirm(true);

        DqlRecoveryPreviewVo preview = service.preview(request, user());
        DqlRecoveryBatchDto result = service.start(request, user());

        assertEquals(List.of("DQL-1", "DQL-2", "DQL-3"), preview.getOrderedEvents().stream()
                .map(DqlRecoveryPreviewVo.OrderedEvent::getEventId).toList());
        assertEquals(List.of("DQL-1", "DQL-2", "DQL-3"), result.getOrderedEventIds());
        ArgumentCaptor<Map<String, Object>> payload = ArgumentCaptor.forClass(Map.class);
        verify(messageQueueService).sendPipeMessage(payload.capture(), eq("tm"), eq("agent-1"));
        assertEquals(List.of("DQL-1", "DQL-2", "DQL-3"), payload.getValue().get("orderedEventIds"));
    }

    @Test
    @DisplayName("start releases locks and fails the batch when not every event is locked")
    void startCompensatesWhenLockCountDoesNotMatch() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryTaskLockRepository taskLockRepository = mock(DqlRecoveryTaskLockRepository.class);
        DqlRecoveryBatchService service = lockedService(eventRepository, batchRepository,
                mock(DqlEventAlarmService.class), taskLockRepository, mock(MessageQueueServiceImpl.class));
        DqlEventDto event = event("DQL-1", TASK_ID, DqlEventStatusEnum.PENDING, "agent-1");
        when(eventRepository.findByEventIds(List.of("DQL-1"))).thenReturn(List.of(event));
        when(batchRepository.create(any(DqlRecoveryBatchDto.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(taskLockRepository.tryAcquire(eq(TASK_ID), anyString(), anyLong())).thenReturn(true);
        when(eventRepository.lockEvents(eq(List.of("DQL-1")), anyString())).thenReturn(0L);
        DqlRecoveryRequestVo request = request(List.of("DQL-1"));
        request.setConfirm(true);

        BizException exception = assertThrows(BizException.class, () -> service.start(request, user()));

        assertEquals("DqlRecovery.EventLockFailed", exception.getErrorCode());
        verify(eventRepository).releaseBatchLocks(anyString(), eq(Map.of(
                "DQL-1", DqlEventStatusEnum.PENDING)));
        verify(batchRepository).finish(anyString(), eq(DqlRecoveryBatchStatusEnum.CANCELED), eq("Failed to lock selected events"));
        verify(taskLockRepository).release(eq(TASK_ID), anyString());
    }

    @Test
    @DisplayName("partial event locking restores each selected event's original status")
    void startRestoresOriginalStatusAfterPartialLock() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryTaskLockRepository taskLockRepository = mock(DqlRecoveryTaskLockRepository.class);
        DqlRecoveryBatchService service = lockedService(eventRepository, batchRepository,
                mock(DqlEventAlarmService.class), taskLockRepository, mock(MessageQueueServiceImpl.class));
        DqlEventDto pending = event("DQL-1", TASK_ID, DqlEventStatusEnum.PENDING, "agent-1");
        DqlEventDto failed = event("DQL-2", TASK_ID, DqlEventStatusEnum.RECOVERY_FAILED, "agent-1");
        when(eventRepository.findByEventIds(anyList())).thenReturn(List.of(pending, failed));
        when(batchRepository.create(any(DqlRecoveryBatchDto.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(taskLockRepository.tryAcquire(eq(TASK_ID), anyString(), anyLong())).thenReturn(true);
        when(eventRepository.lockEvents(eq(List.of("DQL-1", "DQL-2")), anyString())).thenReturn(1L);
        DqlRecoveryRequestVo request = request(List.of("DQL-1", "DQL-2"));
        request.setConfirm(true);

        assertThrows(BizException.class, () -> service.start(request, user()));

        verify(eventRepository).releaseBatchLocks(anyString(), eq(Map.of(
                "DQL-1", DqlEventStatusEnum.PENDING,
                "DQL-2", DqlEventStatusEnum.RECOVERY_FAILED)));
        verify(batchRepository).finish(anyString(), eq(DqlRecoveryBatchStatusEnum.CANCELED), eq("Failed to lock selected events"));
    }

    @Test
    @DisplayName("event lock failure compensates the created batch and any acquired event locks")
    void startCompensatesWhenEventLockThrows() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryTaskLockRepository taskLockRepository = mock(DqlRecoveryTaskLockRepository.class);
        DqlRecoveryBatchService service = lockedService(eventRepository, batchRepository,
                mock(DqlEventAlarmService.class), taskLockRepository, mock(MessageQueueServiceImpl.class));
        when(eventRepository.findByEventIds(List.of("DQL-1")))
                .thenReturn(List.of(event("DQL-1", TASK_ID, DqlEventStatusEnum.RECOVERY_FAILED, "agent-1")));
        when(batchRepository.create(any(DqlRecoveryBatchDto.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(taskLockRepository.tryAcquire(eq(TASK_ID), anyString(), anyLong())).thenReturn(true);
        when(eventRepository.lockEvents(eq(List.of("DQL-1")), anyString()))
                .thenThrow(new RuntimeException("event store unavailable"));
        DqlRecoveryRequestVo request = request(List.of("DQL-1"));
        request.setConfirm(true);

        assertThrows(RuntimeException.class, () -> service.start(request, user()));

        verify(eventRepository).releaseBatchLocks(anyString(), eq(Map.of(
                "DQL-1", DqlEventStatusEnum.RECOVERY_FAILED)));
        verify(batchRepository).finish(anyString(), eq(DqlRecoveryBatchStatusEnum.CANCELED), eq("event store unavailable"));
        verify(taskLockRepository).release(eq(TASK_ID), anyString());
    }

    @Test
    @DisplayName("start rejects an atomic task lock conflict before creating a batch")
    void startRejectsTaskLockConflict() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryTaskLockRepository taskLockRepository = mock(DqlRecoveryTaskLockRepository.class);
        DqlRecoveryBatchService service = lockedService(eventRepository, batchRepository,
                mock(DqlEventAlarmService.class), taskLockRepository, mock(MessageQueueServiceImpl.class));
        when(eventRepository.findByEventIds(List.of("DQL-1")))
                .thenReturn(List.of(event("DQL-1", TASK_ID, DqlEventStatusEnum.PENDING, "agent-1")));
        when(taskLockRepository.tryAcquire(eq(TASK_ID), anyString(), anyLong())).thenReturn(false);
        DqlRecoveryRequestVo request = request(List.of("DQL-1"));
        request.setConfirm(true);

        BizException exception = assertThrows(BizException.class, () -> service.start(request, user()));

        assertEquals("DqlRecovery.BatchAlreadyRunning", exception.getErrorCode());
        verify(batchRepository, never()).create(any(DqlRecoveryBatchDto.class));
        verify(eventRepository, never()).lockEvents(anyList(), anyString());
    }

    @Test
    @DisplayName("start releases the task lock when batch persistence returns no batch")
    void startReleasesTaskLockWhenBatchCreationReturnsNull() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryTaskLockRepository taskLockRepository = mock(DqlRecoveryTaskLockRepository.class);
        DqlRecoveryBatchService service = lockedService(eventRepository, batchRepository,
                mock(DqlEventAlarmService.class), taskLockRepository, mock(MessageQueueServiceImpl.class));
        when(eventRepository.findByEventIds(List.of("DQL-1")))
                .thenReturn(List.of(event("DQL-1", TASK_ID, DqlEventStatusEnum.PENDING, "agent-1")));
        when(taskLockRepository.tryAcquire(eq(TASK_ID), anyString(), anyLong())).thenReturn(true);
        when(batchRepository.create(any(DqlRecoveryBatchDto.class))).thenReturn(null);
        DqlRecoveryRequestVo request = request(List.of("DQL-1"));
        request.setConfirm(true);

        assertThrows(IllegalStateException.class, () -> service.start(request, user()));

        verify(taskLockRepository).release(eq(TASK_ID), anyString());
    }

    @Test
    @DisplayName("start releases pending locks and marks the batch failed when dispatch fails")
    void startCompensatesWhenDispatchFails() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        MessageQueueServiceImpl messageQueueService = mock(MessageQueueServiceImpl.class);
        DqlRecoveryTaskLockRepository taskLockRepository = mock(DqlRecoveryTaskLockRepository.class);
        DqlRecoveryBatchService service = lockedService(eventRepository, batchRepository,
                mock(DqlEventAlarmService.class), taskLockRepository, messageQueueService);
        DqlEventDto event = event("DQL-1", TASK_ID, DqlEventStatusEnum.PENDING, "agent-1");
        when(eventRepository.findByEventIds(List.of("DQL-1"))).thenReturn(List.of(event));
        when(batchRepository.create(any(DqlRecoveryBatchDto.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(taskLockRepository.tryAcquire(eq(TASK_ID), anyString(), anyLong())).thenReturn(true);
        when(eventRepository.lockEvents(eq(List.of("DQL-1")), anyString())).thenReturn(1L);
        RuntimeException cause = new RuntimeException("queue unavailable");
        doThrow(cause).when(messageQueueService).sendPipeMessage(any(), eq("tm"), eq("agent-1"));
        DqlRecoveryRequestVo request = request(List.of("DQL-1"));
        request.setConfirm(true);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> service.start(request, user()));

        assertSame(cause, exception);
        verify(eventRepository).releaseBatchLocks(anyString(), eq(Map.of(
                "DQL-1", DqlEventStatusEnum.PENDING)));
        verify(batchRepository).finish(anyString(), eq(DqlRecoveryBatchStatusEnum.FAILED), eq("queue unavailable"));
        verify(taskLockRepository).release(eq(TASK_ID), anyString());
    }

    @Test
    @DisplayName("batch failure report releases event locks and records a failed batch")
    void batchFailureReportReleasesLocks() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlEventAlarmService alarmService = mock(DqlEventAlarmService.class);
        DqlRecoveryTaskLockRepository taskLockRepository = mock(DqlRecoveryTaskLockRepository.class);
        DqlRecoveryBatchService service = lockedService(eventRepository, batchRepository, alarmService,
                taskLockRepository, mock(MessageQueueServiceImpl.class));
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1"));
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);
        DqlRecoveryResultReportVo report = report("DQLB-1", null, "BATCH_FAILED");
        report.setMessage("agent stopped");

        service.report(TASK_ID, report);

        verify(eventRepository).finalizeRunningAttempts(eq("DQLB-1"), eq(List.of("DQL-1")),
                eq(DqlRecoveryAttemptResultEnum.FAILED), eq("agent stopped"), any());
        ArgumentCaptor<DqlRecoveryAttemptDto> fallbackAttempt =
                ArgumentCaptor.forClass(DqlRecoveryAttemptDto.class);
        verify(eventRepository).finalizeUnstartedAttempts(
                eq("DQLB-1"), eq(List.of("DQL-1")), fallbackAttempt.capture(), any());
        assertEquals("DQLB-1", fallbackAttempt.getValue().getBatchId());
        assertEquals(DqlRecoveryAttemptResultEnum.FAILED.name(), fallbackAttempt.getValue().getResult());
        assertEquals("agent stopped", fallbackAttempt.getValue().getMessage());
        assertEquals("BATCH_FAILED-DQLB-1", fallbackAttempt.getValue().getAttemptId());
        verify(eventRepository).releaseBatchLocks("DQLB-1", DqlEventStatusEnum.RECOVERY_FAILED);
        verify(batchRepository).finish("DQLB-1", DqlRecoveryBatchStatusEnum.FAILED, "agent stopped");
        verify(alarmService).notifyRecoveryFailed(batch);
        verify(taskLockRepository).release(TASK_ID, "DQLB-1");
    }

    private DqlRecoveryBatchService service(DqlEventRepository eventRepository,
                                            DqlRecoveryBatchRepository batchRepository,
                                            DqlEventAlarmService alarmService) {
        return new DqlRecoveryBatchService(
                eventRepository,
                batchRepository,
                mock(DqlEventPermissionService.class),
                mock(MessageQueueServiceImpl.class),
                alarmService
        );
    }

    private DqlRecoveryBatchService strictService(DqlEventRepository eventRepository,
                                                  DqlRecoveryBatchRepository batchRepository,
                                                  TaskService taskService,
                                                  WorkerService workerService) {
        return new DqlRecoveryBatchService(
                eventRepository,
                batchRepository,
                mock(DqlEventPermissionService.class),
                mock(MessageQueueServiceImpl.class),
                mock(DqlEventAlarmService.class),
                taskService,
                workerService
        );
    }

    private DqlRecoveryBatchService lockedService(DqlEventRepository eventRepository,
                                                   DqlRecoveryBatchRepository batchRepository,
                                                   DqlEventAlarmService alarmService,
                                                   DqlRecoveryTaskLockRepository taskLockRepository,
                                                   MessageQueueServiceImpl messageQueueService) {
        return new DqlRecoveryBatchService(
                eventRepository,
                batchRepository,
                mock(DqlEventPermissionService.class),
                messageQueueService,
                alarmService,
                null,
                null,
                taskLockRepository
        );
    }

    private void prepareReadyTask(TaskService taskService, WorkerService workerService) {
        when(taskService.findByTaskId(eq(new ObjectId(TASK_ID)), eq("name"), eq("status"), eq("version"), eq("agentId")))
                .thenReturn(recoveryTask("running", 7L, "agent-1"));
        prepareReadyWorker(workerService);
    }

    private void prepareReadyWorker(WorkerService workerService) {
        WorkerDto worker = recoveryWorker("agent-1");
        when(workerService.queryWorkerByProcessId("agent-1")).thenReturn(worker);
        when(workerService.isAgentTimeout(worker.getPingTime())).thenReturn(false);
    }

    private DqlEventDto recoveryEvent(String eventId, long taskVersion) {
        DqlEventDto event = event(eventId, TASK_ID, DqlEventStatusEnum.PENDING, "agent-1");
        event.setTaskVersion(taskVersion);
        event.setEventKey(Map.of("id", eventId));
        event.setEventKeyMissing(false);
        event.setRecordIdentity("orders|id=" + eventId);
        return event;
    }

    private TaskDto recoveryTask(String status, long version, String agentId) {
        TaskDto task = new TaskDto();
        task.setName("sync_order");
        task.setStatus(status);
        task.setVersion(version);
        task.setAgentId(agentId);
        return task;
    }

    private WorkerDto recoveryWorker(String processId) {
        WorkerDto worker = new WorkerDto();
        worker.setProcessId(processId);
        worker.setPingTime(System.currentTimeMillis());
        worker.setStopping(false);
        worker.setIsDeleted(false);
        worker.setDeleted(false);
        return worker;
    }

    private DqlRecoveryRequestVo request(List<String> eventIds) {
        DqlRecoveryRequestVo request = new DqlRecoveryRequestVo();
        request.setEventIds(eventIds);
        return request;
    }

    private DqlRecoveryResultReportVo report(String batchId, String eventId, String type) {
        DqlRecoveryResultReportVo report = new DqlRecoveryResultReportVo();
        report.setBatchId(batchId);
        report.setEventId(eventId);
        report.setType(type);
        report.setAttemptId("A-1");
        return report;
    }

    private DqlRecoveryBatchDto batch(String batchId, List<String> eventIds) {
        DqlRecoveryBatchDto batch = new DqlRecoveryBatchDto();
        batch.setBatchId(batchId);
        batch.setTaskId(TASK_ID);
        batch.setEventIds(eventIds);
        batch.setStatus(DqlRecoveryBatchStatusEnum.RUNNING.name());
        batch.setSelectedCount(eventIds.size());
        batch.setSuccessCount(0);
        batch.setFailedCount(0);
        batch.setSkippedCount(0);
        batch.setUpdated(new java.util.Date(0L));
        return batch;
    }

    private Object getUnchecked(Future<Object> future) {
        try {
            return future.get(5, TimeUnit.SECONDS);
        } catch (Exception exception) {
            throw new AssertionError("concurrent recovery start did not complete", exception);
        }
    }

    private DqlEventDto event(String eventId, String taskId, DqlEventStatusEnum status, String agentId) {
        DqlEventDto event = new DqlEventDto();
        event.setEventId(eventId);
        event.setTaskId(taskId);
        event.setTaskName("sync_order");
        event.setStatus(status.name());
        event.setAgentId(agentId);
        event.setEventTime(new java.util.Date(1000L));
        event.setCaptureSeq(1L);
        event.setPayloadComplete(true);
        return event;
    }

    private UserDetail user() {
        return new UserDetail("user-id", "customer-id", "Harsen", "password", Collections.<SimpleGrantedAuthority>emptyList());
    }
}
