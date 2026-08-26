package com.tapdata.tm.dql.service;

import com.tapdata.tm.base.dto.Page;
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
import com.tapdata.tm.dql.vo.DqlEventDetailVo;
import com.tapdata.tm.dql.vo.DqlEventQueryVo;
import com.tapdata.tm.dql.vo.DqlEventReportResultVo;
import com.tapdata.tm.dql.vo.DqlEventReportVo;
import com.tapdata.tm.dql.vo.DqlEventSummaryVo;
import com.tapdata.tm.dql.vo.DqlRecoveryPreviewVo;
import com.tapdata.tm.dql.vo.DqlRecoveryRequestVo;
import com.tapdata.tm.dql.vo.DqlRecoveryResultReportVo;
import com.tapdata.tm.messagequeue.service.MessageQueueServiceImpl;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DqlEventServiceTest {
    private static final String TASK_ID = "64f000000000000000000001";

    @Test
    @DisplayName("report assigns pending status and generated event id before persisting a new event")
    void reportAssignsPendingStatusAndGeneratedEventId() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventAlarmService alarmService = mock(DqlEventAlarmService.class);
        DqlEventService service = new DqlEventService(eventRepository, alarmService);
        DqlEventReportVo report = reportVo();
        report.setCaptureSeq(null);
        report.setEventIdentity(null);
        when(eventRepository.nextCaptureSeq(TASK_ID)).thenReturn(42L);
        when(eventRepository.findDuplicate(eq(TASK_ID), any())).thenReturn(null);
        when(eventRepository.upsert(any(DqlEventDto.class))).thenAnswer(invocation -> invocation.getArgument(0));

        DqlEventReportResultVo result = service.report(TASK_ID, report);

        assertEquals("DQL-64f000-000042", result.getEventId());
        assertEquals(DqlEventStatusEnum.PENDING.name(), result.getStatus());
        assertFalse(result.isDuplicate());
        verify(alarmService).notifyEventCreated(any(DqlEventDto.class));
    }

    @Test
    @DisplayName("report returns duplicate event without persisting or notifying again")
    void reportReturnsDuplicateWithoutPersistingAgain() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventAlarmService alarmService = mock(DqlEventAlarmService.class);
        DqlEventService service = new DqlEventService(eventRepository, alarmService);
        DqlEventDto duplicate = event("DQL-64f000-000001", TASK_ID, 10L, DqlEventStatusEnum.PENDING);
        when(eventRepository.findDuplicate(eq(TASK_ID), any())).thenReturn(duplicate);

        DqlEventReportResultVo result = service.report(TASK_ID, reportVo());

        assertEquals("DQL-64f000-000001", result.getEventId());
        assertEquals(DqlEventStatusEnum.PENDING.name(), result.getStatus());
        assertTrue(result.isDuplicate());
        verify(eventRepository, never()).upsert(any(DqlEventDto.class));
        verify(alarmService, never()).notifyEventCreated(any(DqlEventDto.class));
    }

    @Test
    @DisplayName("summary counts each DLQ status using the same query object")
    void summaryCountsStatuses() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventService service = new DqlEventService(eventRepository, mock(DqlEventAlarmService.class));
        DqlEventQueryVo query = new DqlEventQueryVo();
        when(eventRepository.count(query)).thenReturn(42L);
        when(eventRepository.countByStatus(query, DqlEventStatusEnum.PENDING)).thenReturn(12L);
        when(eventRepository.countByStatus(query, DqlEventStatusEnum.REPROCESSING)).thenReturn(1L);
        when(eventRepository.countByStatus(query, DqlEventStatusEnum.RECOVERED)).thenReturn(20L);
        when(eventRepository.countByStatus(query, DqlEventStatusEnum.RECOVERY_FAILED)).thenReturn(8L);
        when(eventRepository.countByStatus(query, DqlEventStatusEnum.NOT_REPROCESSABLE)).thenReturn(1L);

        DqlEventSummaryVo summary = service.summary(query, user());

        assertEquals(42L, summary.getTotal());
        assertEquals(12L, summary.getPending());
        assertEquals(1L, summary.getReprocessing());
        assertEquals(20L, summary.getRecovered());
        assertEquals(8L, summary.getRecoveryFailed());
        assertEquals(1L, summary.getNotReprocessable());
    }

    @Test
    @DisplayName("preview rejects selected events from different tasks")
    void previewRejectsCrossTaskEvents() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchService service = recoveryService(eventRepository, mock(DqlRecoveryBatchRepository.class));
        when(eventRepository.findByEventIds(List.of("DQL-1", "DQL-2")))
                .thenReturn(List.of(
                        event("DQL-1", TASK_ID, 2L, DqlEventStatusEnum.PENDING),
                        event("DQL-2", "64f000000000000000000002", 1L, DqlEventStatusEnum.PENDING)
                ));
        DqlRecoveryRequestVo request = new DqlRecoveryRequestVo();
        request.setEventIds(List.of("DQL-1", "DQL-2"));

        BizException exception = assertThrows(BizException.class, () -> service.preview(request, user()));

        assertEquals("DqlRecovery.CrossTaskNotAllowed", exception.getErrorCode());
    }

    @Test
    @DisplayName("preview orders recoverable events by event time, capture sequence, then event id")
    void previewOrdersRecoverableEvents() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchService service = recoveryService(eventRepository, mock(DqlRecoveryBatchRepository.class));
        DqlEventDto later = event("DQL-64f000-000003", TASK_ID, 3L, DqlEventStatusEnum.PENDING);
        later.setEventTime(new Date(2000));
        DqlEventDto earlier = event("DQL-64f000-000001", TASK_ID, 1L, DqlEventStatusEnum.PENDING);
        earlier.setEventTime(new Date(1000));
        DqlEventDto blocked = event("DQL-64f000-000002", TASK_ID, 2L, DqlEventStatusEnum.RECOVERED);
        blocked.setEventTime(new Date(1000));
        when(eventRepository.findByEventIds(List.of("DQL-64f000-000003", "DQL-64f000-000001", "DQL-64f000-000002")))
                .thenReturn(List.of(later, earlier, blocked));
        DqlRecoveryRequestVo request = new DqlRecoveryRequestVo();
        request.setEventIds(List.of("DQL-64f000-000003", "DQL-64f000-000001", "DQL-64f000-000002"));

        DqlRecoveryPreviewVo preview = service.preview(request, user());

        assertFalse(preview.isCanSubmit());
        assertEquals(List.of("DQL-64f000-000001", "DQL-64f000-000003"),
                preview.getOrderedEvents().stream().map(DqlRecoveryPreviewVo.OrderedEvent::getEventId).toList());
        assertEquals("DQL-64f000-000002", preview.getBlockedEvents().get(0).getEventId());
        assertEquals("status RECOVERED is not reprocessable", preview.getBlockedEvents().get(0).getReason());
    }

    @Test
    @DisplayName("event result callback marks a recovery event successful and updates batch counters")
    void reportEventResultSuccess() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryBatchService service = recoveryService(eventRepository, batchRepository);
        DqlRecoveryBatchDto batch = new DqlRecoveryBatchDto();
        batch.setBatchId("DQLB-20260825-000001");
        batch.setTaskId(TASK_ID);
        batch.setEventIds(List.of("DQL-64f000-000001"));
        when(batchRepository.findByBatchId("DQLB-20260825-000001")).thenReturn(batch);
        when(eventRepository.completeEvent(eq("DQL-64f000-000001"), eq("DQLB-20260825-000001"), any())).thenReturn(true);
        DqlRecoveryResultReportVo report = new DqlRecoveryResultReportVo();
        report.setBatchId("DQLB-20260825-000001");
        report.setEventId("DQL-64f000-000001");
        report.setAttemptId("A-000001");
        report.setType("EVENT_RESULT");
        report.setResult(DqlRecoveryAttemptResultEnum.SUCCESS.name());

        service.report(TASK_ID, report);

        verify(eventRepository).completeEvent(eq("DQL-64f000-000001"), eq("DQLB-20260825-000001"), any());
        verify(batchRepository).increaseSuccess("DQLB-20260825-000001");
    }

    @Test
    @DisplayName("start recovery requires explicit frontend confirmation")
    void startRecoveryRequiresExplicitConfirm() {
        DqlRecoveryBatchService service = recoveryService(mock(DqlEventRepository.class), mock(DqlRecoveryBatchRepository.class));
        DqlRecoveryRequestVo request = new DqlRecoveryRequestVo();
        request.setEventIds(List.of("DQL-64f000-000001"));

        BizException exception = assertThrows(BizException.class, () -> service.start(request, user()));

        assertEquals("IllegalArgument", exception.getErrorCode());
    }

    @Test
    @DisplayName("page delegates query to repository and preserves pagination result")
    void pageDelegatesToRepository() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventService service = new DqlEventService(eventRepository, mock(DqlEventAlarmService.class));
        DqlEventQueryVo query = new DqlEventQueryVo();
        Page<DqlEventDto> page = Page.page(List.of(event("DQL-64f000-000001", TASK_ID, 1L, DqlEventStatusEnum.PENDING)), 1);
        when(eventRepository.page(query)).thenReturn(page);

        assertEquals(page, service.page(query, user()));
    }

    @Test
    @DisplayName("page response omits full payload and attempt history from list items")
    void pageOmitsPayloadAndAttempts() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventService service = new DqlEventService(eventRepository, mock(DqlEventAlarmService.class));
        DqlEventQueryVo query = new DqlEventQueryVo();
        DqlEventDto event = event("DQL-64f000-000001", TASK_ID, 1L, DqlEventStatusEnum.PENDING);
        event.setPayloadData(Map.of("after", Map.of("id", 1001)));
        event.setRecoveryAttempts(List.of(new com.tapdata.tm.dql.dto.DqlRecoveryAttemptDto()));
        when(eventRepository.page(query)).thenReturn(Page.page(List.of(event), 1));

        Page<DqlEventDto> page = service.page(query, user());

        assertNull(page.getItems().get(0).getPayloadData());
        assertNull(page.getItems().get(0).getRecoveryAttempts());
    }

    @Test
    @DisplayName("detail response includes current batch for reprocessing events")
    void detailIncludesCurrentBatchForReprocessingEvent() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlEventService service = new DqlEventService(eventRepository, mock(DqlEventAlarmService.class), null, batchRepository);
        DqlEventDto event = event("DQL-64f000-000001", TASK_ID, 1L, DqlEventStatusEnum.REPROCESSING);
        event.setCurrentBatchId("DQLB-20260826-000001");
        event.setPayloadData(Map.of("after", Map.of("id", 1001)));
        DqlRecoveryBatchDto batch = new DqlRecoveryBatchDto();
        batch.setBatchId("DQLB-20260826-000001");
        batch.setStatus(DqlRecoveryBatchStatusEnum.RUNNING.name());
        when(eventRepository.findByEventId("DQL-64f000-000001")).thenReturn(event);
        when(batchRepository.findByBatchId("DQLB-20260826-000001")).thenReturn(batch);

        DqlEventDetailVo detail = service.detail("DQL-64f000-000001", user());

        assertNull(detail.getPayloadData());
        assertNotNull(detail.getCurrentBatch());
        assertEquals("DQLB-20260826-000001", detail.getCurrentBatch().getBatchId());
        assertEquals(DqlRecoveryBatchStatusEnum.RUNNING.name(), detail.getCurrentBatch().getStatus());
    }

    private static DqlRecoveryBatchService recoveryService(DqlEventRepository eventRepository, DqlRecoveryBatchRepository batchRepository) {
        return new DqlRecoveryBatchService(
                eventRepository,
                batchRepository,
                mock(DqlEventPermissionService.class),
                mock(MessageQueueServiceImpl.class),
                mock(DqlEventAlarmService.class)
        );
    }

    private static DqlEventReportVo reportVo() {
        DqlEventReportVo report = new DqlEventReportVo();
        report.setTaskRecordId("record-1");
        report.setTaskName("sync_order");
        report.setTaskVersion(7L);
        report.setAgentId("agent-1");
        report.setSourceNodeId("source-node");
        report.setSourceNodeName("mysql_src");
        report.setFailedNodeId("js-node");
        report.setFailedNodeName("JS Processor");
        report.setFailedStage("PROCESSOR");
        report.setSourceTable("orders");
        report.setTargetTable("orders_sink");
        report.setTableId("orders");
        report.setDmlType("U");
        report.setEventTime(1787580000000L);
        report.setEventKey(Map.of("id", 1001));
        report.setEventIdentity("sha256:identity");
        report.setPayloadFormat("tap-record-event-json-v1");
        report.setPayloadData(Map.of("after", Map.of("id", 1001)));
        report.setPayloadHash("sha256:payload");
        report.setPayloadSize(128L);
        report.setPayloadComplete(true);
        report.setPayloadPreview(Map.of("after", Map.of("id", 1001)));
        report.setErrorType("TRANSFORM_ERROR");
        report.setErrorCode("JS_PROCESS_FAILED");
        report.setErrorDetails("script failed");
        return report;
    }

    private static DqlEventDto event(String eventId, String taskId, long captureSeq, DqlEventStatusEnum status) {
        DqlEventDto event = new DqlEventDto();
        event.setEventId(eventId);
        event.setTaskId(taskId);
        event.setTaskName("sync_order");
        event.setSourceTable("orders");
        event.setTargetTable("orders_sink");
        event.setTableId("orders");
        event.setDmlType("U");
        event.setErrorType("TRANSFORM_ERROR");
        event.setErrorCode("JS_PROCESS_FAILED");
        event.setStatus(status.name());
        event.setCaptureSeq(captureSeq);
        event.setEventTime(new Date(1000 + captureSeq));
        event.setPayloadComplete(true);
        return event;
    }

    private static UserDetail user() {
        return new UserDetail("user-id", "customer-id", "Harsen", "password", Collections.<SimpleGrantedAuthority>emptyList());
    }
}
