package com.tapdata.tm.dql.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dql.DqlEventStatusEnum;
import com.tapdata.tm.dql.DqlRecoveryAttemptResultEnum;
import com.tapdata.tm.dql.DqlRecoveryBatchStatusEnum;
import com.tapdata.tm.dql.dto.DqlEventDto;
import com.tapdata.tm.dql.dto.DqlRecoveryBatchDto;
import com.tapdata.tm.dql.repository.DqlEventRepository;
import com.tapdata.tm.dql.repository.DqlRecoveryBatchRepository;
import com.tapdata.tm.dql.vo.DqlRecoveryPreviewVo;
import com.tapdata.tm.dql.vo.DqlRecoveryRequestVo;
import com.tapdata.tm.dql.vo.DqlRecoveryResultReportVo;
import com.tapdata.tm.messagequeue.service.MessageQueueServiceImpl;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
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
        when(eventRepository.failEvent(eq("DQL-1"), eq("DQLB-1"), any())).thenReturn(true);

        DqlRecoveryResultReportVo report = report("DQLB-1", "DQL-1", "EVENT_RESULT");
        report.setResult(DqlRecoveryAttemptResultEnum.SKIPPED.name());

        service.report(TASK_ID, report);

        verify(batchRepository).increaseSkipped("DQLB-1");
        verify(batchRepository, never()).increaseFailed("DQLB-1");
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
    @DisplayName("start dispatches the server ordered event ids and marks the batch dispatched")
    void startDispatchesOrderedEvents() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        MessageQueueServiceImpl messageQueueService = mock(MessageQueueServiceImpl.class);
        DqlEventPermissionService permissionService = mock(DqlEventPermissionService.class);
        DqlRecoveryBatchService service = new DqlRecoveryBatchService(
                eventRepository, batchRepository, permissionService, messageQueueService, mock(DqlEventAlarmService.class));
        DqlEventDto event = event("DQL-1", TASK_ID, DqlEventStatusEnum.PENDING, "agent-1");
        when(eventRepository.findByEventIds(List.of("DQL-1"))).thenReturn(List.of(event));
        when(batchRepository.create(any(DqlRecoveryBatchDto.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(eventRepository.lockEvents(eq(List.of("DQL-1")), anyString())).thenReturn(1L);
        DqlRecoveryRequestVo request = request(List.of("DQL-1"));
        request.setConfirm(true);

        DqlRecoveryBatchDto result = service.start(request, user());

        assertEquals(DqlRecoveryBatchStatusEnum.DISPATCHED.name(), result.getStatus());
        verify(batchRepository).updateStatus(anyString(), eq(DqlRecoveryBatchStatusEnum.DISPATCHED), eq(null));
        ArgumentCaptor<Map<String, Object>> payload = ArgumentCaptor.forClass(Map.class);
        verify(messageQueueService).sendPipeMessage(payload.capture(), eq("tm"), eq("agent-1"));
        assertEquals("dqlRecovery", payload.getValue().get("type"));
        assertEquals(TASK_ID, payload.getValue().get("taskId"));
        assertEquals(List.of("DQL-1"), payload.getValue().get("eventIds"));
    }

    @Test
    @DisplayName("start releases locks and fails the batch when not every event is locked")
    void startCompensatesWhenLockCountDoesNotMatch() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryBatchService service = service(eventRepository, batchRepository, mock(DqlEventAlarmService.class));
        DqlEventDto event = event("DQL-1", TASK_ID, DqlEventStatusEnum.PENDING, "agent-1");
        when(eventRepository.findByEventIds(List.of("DQL-1"))).thenReturn(List.of(event));
        when(batchRepository.create(any(DqlRecoveryBatchDto.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(eventRepository.lockEvents(eq(List.of("DQL-1")), anyString())).thenReturn(0L);
        DqlRecoveryRequestVo request = request(List.of("DQL-1"));
        request.setConfirm(true);

        BizException exception = assertThrows(BizException.class, () -> service.start(request, user()));

        assertEquals("DqlRecovery.EventLockFailed", exception.getErrorCode());
        verify(eventRepository).releaseBatchLocks(anyString(), eq(DqlEventStatusEnum.PENDING));
        verify(batchRepository).finish(anyString(), eq(DqlRecoveryBatchStatusEnum.FAILED), eq("Failed to lock selected events"));
    }

    @Test
    @DisplayName("start releases pending locks and marks the batch failed when dispatch fails")
    void startCompensatesWhenDispatchFails() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        MessageQueueServiceImpl messageQueueService = mock(MessageQueueServiceImpl.class);
        DqlRecoveryBatchService service = new DqlRecoveryBatchService(
                eventRepository, batchRepository, mock(DqlEventPermissionService.class),
                messageQueueService, mock(DqlEventAlarmService.class));
        DqlEventDto event = event("DQL-1", TASK_ID, DqlEventStatusEnum.PENDING, "agent-1");
        when(eventRepository.findByEventIds(List.of("DQL-1"))).thenReturn(List.of(event));
        when(batchRepository.create(any(DqlRecoveryBatchDto.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(eventRepository.lockEvents(eq(List.of("DQL-1")), anyString())).thenReturn(1L);
        RuntimeException cause = new RuntimeException("queue unavailable");
        doThrow(cause).when(messageQueueService).sendPipeMessage(any(), eq("tm"), eq("agent-1"));
        DqlRecoveryRequestVo request = request(List.of("DQL-1"));
        request.setConfirm(true);

        RuntimeException exception = assertThrows(RuntimeException.class, () -> service.start(request, user()));

        assertSame(cause, exception);
        verify(eventRepository).releaseBatchLocks(anyString(), eq(DqlEventStatusEnum.PENDING));
        verify(batchRepository).finish(anyString(), eq(DqlRecoveryBatchStatusEnum.FAILED), eq("queue unavailable"));
    }

    @Test
    @DisplayName("batch failure report releases event locks and records a failed batch")
    void batchFailureReportReleasesLocks() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlEventAlarmService alarmService = mock(DqlEventAlarmService.class);
        DqlRecoveryBatchService service = service(eventRepository, batchRepository, alarmService);
        DqlRecoveryBatchDto batch = batch("DQLB-1", List.of("DQL-1"));
        when(batchRepository.findByBatchId("DQLB-1")).thenReturn(batch);
        DqlRecoveryResultReportVo report = report("DQLB-1", null, "BATCH_FAILED");
        report.setMessage("agent stopped");

        service.report(TASK_ID, report);

        verify(eventRepository).releaseBatchLocks("DQLB-1", DqlEventStatusEnum.RECOVERY_FAILED);
        verify(batchRepository).finish("DQLB-1", DqlRecoveryBatchStatusEnum.FAILED, "agent stopped");
        verify(alarmService).notifyRecoveryFailed(batch);
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
        batch.setSelectedCount(eventIds.size());
        batch.setSuccessCount(0);
        batch.setFailedCount(0);
        batch.setSkippedCount(0);
        return batch;
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
