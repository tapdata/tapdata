package com.tapdata.tm.dql.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dql.DqlEventStatusEnum;
import com.tapdata.tm.dql.DqlErrorTypeEnum;
import com.tapdata.tm.dql.DqlRecordIdentityTypeEnum;
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
import com.tapdata.tm.dql.vo.DqlRecordSuccessReportResultVo;
import com.tapdata.tm.dql.vo.DqlRecordSuccessReportVo;
import com.tapdata.tm.dql.vo.DqlRecoveryPreviewVo;
import com.tapdata.tm.dql.vo.DqlRecoveryRequestVo;
import com.tapdata.tm.dql.vo.DqlRecoveryResultReportVo;
import com.tapdata.tm.messagequeue.service.MessageQueueServiceImpl;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

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
import static org.mockito.ArgumentCaptor.forClass;
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
        ArgumentCaptor<DqlEventDto> eventCaptor = forClass(DqlEventDto.class);
        verify(eventRepository).upsert(eventCaptor.capture());
        assertEquals(eventCaptor.getValue().getCreated(), eventCaptor.getValue().getTtlAt());
        assertEquals("target-node", eventCaptor.getValue().getTargetNodeId());
        assertEquals("postgres_sink", eventCaptor.getValue().getTargetNodeName());
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
    @DisplayName("report persists record identity fields used for later success risk detection")
    void reportPersistsRecordIdentityFields() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventService service = new DqlEventService(eventRepository, mock(DqlEventAlarmService.class));
        DqlEventReportVo report = reportVo();
        report.setRecordIdentity("key:orders:id=1001");
        report.setRecordIdentityType(DqlRecordIdentityTypeEnum.PRIMARY_KEY.name());
        report.setRecordIdentityFields(List.of("id"));
        when(eventRepository.findDuplicate(eq(TASK_ID), any())).thenReturn(null);
        when(eventRepository.upsert(any(DqlEventDto.class))).thenAnswer(invocation -> invocation.getArgument(0));

        service.report(TASK_ID, report);

        ArgumentCaptor<DqlEventDto> captor = forClass(DqlEventDto.class);
        verify(eventRepository).upsert(captor.capture());
        assertEquals("key:orders:id=1001", captor.getValue().getRecordIdentity());
        assertEquals(DqlRecordIdentityTypeEnum.PRIMARY_KEY.name(), captor.getValue().getRecordIdentityType());
        assertEquals(List.of("id"), captor.getValue().getRecordIdentityFields());
    }

    @Test
    @DisplayName("report normalizes the legacy target constraint error before persistence")
    void reportNormalizesLegacyTargetConstraintError() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventService service = new DqlEventService(eventRepository, mock(DqlEventAlarmService.class));
        DqlEventReportVo report = reportVo();
        report.setErrorType("TARGET_CONSTRAINT_ERROR");
        when(eventRepository.findDuplicate(eq(TASK_ID), any())).thenReturn(null);
        when(eventRepository.upsert(any(DqlEventDto.class))).thenAnswer(invocation -> invocation.getArgument(0));

        service.report(TASK_ID, report);

        ArgumentCaptor<DqlEventDto> captor = forClass(DqlEventDto.class);
        verify(eventRepository).upsert(captor.capture());
        assertEquals(DqlErrorTypeEnum.TARGET_WRITE_ERROR.name(), captor.getValue().getErrorType());
    }

    @Test
    @DisplayName("report persists route classification metadata used by frontend detail")
    void reportPersistsRouteClassificationMetadata() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventService service = new DqlEventService(eventRepository, mock(DqlEventAlarmService.class));
        DqlEventReportVo report = reportVo();
        report.setExceptionScope("RECORD");
        report.setRouteDecision("RECORD_DLQ");
        report.setClassificationReason("JS process failed on single TapRecordEvent");
        report.setClassificationConfidence("RULE");
        when(eventRepository.findDuplicate(eq(TASK_ID), any())).thenReturn(null);
        when(eventRepository.upsert(any(DqlEventDto.class))).thenAnswer(invocation -> invocation.getArgument(0));

        service.report(TASK_ID, report);

        ArgumentCaptor<DqlEventDto> captor = forClass(DqlEventDto.class);
        verify(eventRepository).upsert(captor.capture());
        assertEquals("RECORD", captor.getValue().getExceptionScope());
        assertEquals("RECORD_DLQ", captor.getValue().getRouteDecision());
        assertEquals("JS process failed on single TapRecordEvent", captor.getValue().getClassificationReason());
        assertEquals("RULE", captor.getValue().getClassificationConfidence());
    }

    @Test
    @DisplayName("report rejects explicit non DLQ route metadata")
    void reportRejectsExplicitNonDqlRouteMetadata() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventService service = new DqlEventService(eventRepository, mock(DqlEventAlarmService.class));
        DqlEventReportVo report = reportVo();
        report.setExceptionScope("TASK_SHARED");
        report.setRouteDecision("TASK_RETRY");
        when(eventRepository.findDuplicate(eq(TASK_ID), any())).thenReturn(null);
        when(eventRepository.upsert(any(DqlEventDto.class))).thenAnswer(invocation -> invocation.getArgument(0));

        BizException exception = assertThrows(BizException.class, () -> service.report(TASK_ID, report));

        assertEquals("DqlEvent.InvalidRouteDecision", exception.getErrorCode());
        verify(eventRepository, never()).upsert(any(DqlEventDto.class));
    }

    @Test
    @DisplayName("report defaults missing route metadata to record DLQ")
    void reportDefaultsMissingRouteMetadataToRecordDql() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventService service = new DqlEventService(eventRepository, mock(DqlEventAlarmService.class));
        DqlEventReportVo report = reportVo();
        report.setExceptionScope(null);
        report.setRouteDecision(null);
        when(eventRepository.findDuplicate(eq(TASK_ID), any())).thenReturn(null);
        when(eventRepository.upsert(any(DqlEventDto.class))).thenAnswer(invocation -> invocation.getArgument(0));

        service.report(TASK_ID, report);

        ArgumentCaptor<DqlEventDto> captor = forClass(DqlEventDto.class);
        verify(eventRepository).upsert(captor.capture());
        assertEquals("RECORD", captor.getValue().getExceptionScope());
        assertEquals("RECORD_DLQ", captor.getValue().getRouteDecision());
    }

    @Test
    @DisplayName("report persists the secured summary and not reprocessable status for oversized payload")
    void reportPersistsSecuredOversizedPayloadSummary() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventService service = new DqlEventService(eventRepository, mock(DqlEventAlarmService.class));
        DqlEventReportVo report = reportVo();
        report.setCaptureSeq(7L);
        report.setPayloadSize(1_048_577L);
        report.setPayloadPreview(Map.of("password", "must-not-leak", "id", 1001));
        report.setErrorDetails("token=must-not-leak\n" + "x".repeat(4001));
        when(eventRepository.findDuplicate(eq(TASK_ID), any())).thenReturn(null);
        when(eventRepository.upsert(any(DqlEventDto.class))).thenAnswer(invocation -> invocation.getArgument(0));

        DqlEventReportResultVo result = service.report(TASK_ID, report);

        ArgumentCaptor<DqlEventDto> captor = forClass(DqlEventDto.class);
        verify(eventRepository).upsert(captor.capture());
        DqlEventDto saved = captor.getValue();
        assertEquals(DqlEventStatusEnum.NOT_REPROCESSABLE.name(), result.getStatus());
        assertEquals(DqlEventStatusEnum.NOT_REPROCESSABLE.name(), saved.getStatus());
        assertNull(saved.getPayloadData());
        assertFalse(saved.getPayloadComplete());
        assertEquals("******", saved.getPayloadPreview().get("password"));
        assertEquals(4000, saved.getErrorDetails().length());
        assertFalse(saved.getErrorDetails().contains("must-not-leak"));
        assertTrue(saved.getErrorDetailsTruncated());
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
        earlier.setOverwriteRisk(true);
        earlier.setOverwriteRiskMessage("该事件异常后，同记录后续存在成功执行的事件，继续重放存在数据覆盖风险，请谨慎操作");
        earlier.setLaterSuccessAt(new Date(3000));
        earlier.setLaterSuccessEventTime(new Date(2500));
        earlier.setLaterSuccessCaptureSeq(7L);
        earlier.setLaterSuccessDmlType("U");
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
        DqlRecoveryPreviewVo.OrderedEvent first = preview.getOrderedEvents().get(0);
        assertTrue(first.getOverwriteRisk());
        assertEquals("该事件异常后，同记录后续存在成功执行的事件，继续重放存在数据覆盖风险，请谨慎操作", first.getOverwriteRiskMessage());
        assertEquals(new Date(3000), first.getLaterSuccessAt());
        assertEquals(new Date(2500), first.getLaterSuccessEventTime());
        assertEquals(7L, first.getLaterSuccessCaptureSeq());
        assertEquals("U", first.getLaterSuccessDmlType());
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
    @DisplayName("record success report marks previous unresolved DLQ event as overwrite risk")
    void recordSuccessReportMarksPreviousDqlEventRisk() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventService service = new DqlEventService(eventRepository, mock(DqlEventAlarmService.class));
        DqlRecordSuccessReportVo report = recordSuccessReport();
        DqlEventDto marked = event("DQL-64f000-000001", TASK_ID, 1L, DqlEventStatusEnum.PENDING);
        marked.setOverwriteRisk(true);
        marked.setOverwriteRiskMessage("该事件异常后，同记录后续存在成功执行的事件，继续重放存在数据覆盖风险，请谨慎操作");
        when(eventRepository.markLaterSuccess(eq(TASK_ID), eq(report), any())).thenReturn(marked);

        DqlRecordSuccessReportResultVo result = service.reportRecordSuccess(TASK_ID, report);

        assertTrue(result.isMarked());
        assertEquals("DQL-64f000-000001", result.getEventId());
        assertEquals("该事件异常后，同记录后续存在成功执行的事件，继续重放存在数据覆盖风险，请谨慎操作", result.getOverwriteRiskMessage());
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
        report.setTargetNodeId("target-node");
        report.setTargetNodeName("postgres_sink");
        report.setFailedNodeId("js-node");
        report.setFailedNodeName("JS Processor");
        report.setFailedStage("PROCESSOR");
        report.setSourceTable("orders");
        report.setTargetTable("orders_sink");
        report.setTableId("orders");
        report.setDmlType("U");
        report.setEventTime(1787580000000L);
        report.setEventKey(Map.of("id", 1001));
        report.setRecordIdentity("key:orders:id=1001");
        report.setRecordIdentityType(DqlRecordIdentityTypeEnum.PRIMARY_KEY.name());
        report.setRecordIdentityFields(List.of("id"));
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

    private static DqlRecordSuccessReportVo recordSuccessReport() {
        DqlRecordSuccessReportVo report = new DqlRecordSuccessReportVo();
        report.setTaskRecordId("record-1");
        report.setTableId("orders");
        report.setSourceTable("orders");
        report.setTargetTable("orders_sink");
        report.setRecordIdentity("key:orders:id=1001");
        report.setRecordIdentityType(DqlRecordIdentityTypeEnum.PRIMARY_KEY.name());
        report.setRecordIdentityFields(List.of("id"));
        report.setDmlType("U");
        report.setEventTime(1787580100000L);
        report.setCaptureSeq(12L);
        report.setSuccessAt(1787580102300L);
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
