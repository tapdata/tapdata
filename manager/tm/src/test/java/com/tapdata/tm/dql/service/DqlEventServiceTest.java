package com.tapdata.tm.dql.service;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dql.DqlEventStatusEnum;
import com.tapdata.tm.dql.DqlErrorTypeEnum;
import com.tapdata.tm.dql.DqlRecordIdentityTypeEnum;
import com.tapdata.tm.dql.DqlRecoveryCallbackResultEnum;
import com.tapdata.tm.dql.DqlRecoveryAttemptResultEnum;
import com.tapdata.tm.dql.DqlRecoveryBatchStatusEnum;
import com.tapdata.tm.dql.dto.DqlEventDto;
import com.tapdata.tm.dql.dto.DqlRecoveryAttemptDto;
import com.tapdata.tm.dql.dto.DqlRecoveryBatchDto;
import com.tapdata.tm.dql.repository.DqlEventRepository;
import com.tapdata.tm.dql.repository.DqlRecoveryBatchRepository;
import com.tapdata.tm.dql.vo.DqlEventDetailVo;
import com.tapdata.tm.dql.vo.DqlEventListVo;
import com.tapdata.tm.dql.vo.DqlEventQueryVo;
import com.tapdata.tm.dql.vo.DqlEventReportResultVo;
import com.tapdata.tm.dql.vo.DqlEventReportVo;
import com.tapdata.tm.dql.vo.DqlEventSummaryVo;
import com.tapdata.tm.dql.vo.DqlRecordSuccessReportResultVo;
import com.tapdata.tm.dql.vo.DqlRecordSuccessReportVo;
import com.tapdata.tm.dql.vo.DqlRecoveryPreviewVo;
import com.tapdata.tm.dql.vo.DqlRecoveryPayloadVo;
import com.tapdata.tm.dql.vo.DqlRecoveryRequestVo;
import com.tapdata.tm.dql.vo.DqlRecoveryResultReportVo;
import com.tapdata.tm.messagequeue.service.MessageQueueServiceImpl;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.service.TaskService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.query.Query;
import org.bson.types.ObjectId;

import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
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
    @DisplayName("report rejects an event without a task version")
    void reportRejectsMissingTaskVersion() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventService service = new DqlEventService(eventRepository, mock(DqlEventAlarmService.class));
        DqlEventReportVo report = reportVo();
        report.setTaskVersion(null);
        when(eventRepository.findDuplicate(eq(TASK_ID), any())).thenReturn(null);
        when(eventRepository.nextCaptureSeq(TASK_ID)).thenReturn(42L);
        when(eventRepository.upsert(any(DqlEventDto.class))).thenAnswer(invocation -> invocation.getArgument(0));

        BizException exception = assertThrows(BizException.class, () -> service.report(TASK_ID, report));

        assertEquals("DqlEvent.InvalidTaskVersion", exception.getErrorCode());
        verify(eventRepository, never()).upsert(any(DqlEventDto.class));
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
    @DisplayName("report treats an existing event returned by atomic upsert as a concurrent duplicate")
    void reportTreatsAtomicUpsertRaceAsDuplicate() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventAlarmService alarmService = mock(DqlEventAlarmService.class);
        DqlEventService service = new DqlEventService(eventRepository, alarmService);
        DqlEventDto existing = event("DQL-64f000-000001", TASK_ID, 1L, DqlEventStatusEnum.PENDING);
        when(eventRepository.findDuplicate(eq(TASK_ID), any())).thenReturn(null);
        when(eventRepository.nextCaptureSeq(TASK_ID)).thenReturn(42L);
        when(eventRepository.upsert(any(DqlEventDto.class))).thenReturn(existing);

        DqlEventReportResultVo result = service.report(TASK_ID, reportVo());

        assertEquals("DQL-64f000-000001", result.getEventId());
        assertTrue(result.isDuplicate());
        verify(alarmService, never()).notifyEventCreated(any(DqlEventDto.class));
    }

    @Test
    @DisplayName("report notifies save failure and returns a system error when persistence throws")
    void reportNotifiesSaveFailureWhenPersistenceThrows() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventAlarmService alarmService = mock(DqlEventAlarmService.class);
        DqlEventService service = new DqlEventService(eventRepository, alarmService);
        IllegalStateException cause = new IllegalStateException("database unavailable");
        when(eventRepository.findDuplicate(eq(TASK_ID), any())).thenReturn(null);
        when(eventRepository.upsert(any(DqlEventDto.class))).thenThrow(cause);

        BizException exception = assertThrows(BizException.class, () -> service.report(TASK_ID, reportVo()));

        assertEquals(BizException.SYSTEM_ERROR, exception.getErrorCode());
        assertSame(cause, exception.getCause());
        verify(alarmService).notifySaveFailed(eq(TASK_ID), contains("database unavailable"));
        verify(alarmService, never()).notifyEventCreated(any(DqlEventDto.class));
    }

    @Test
    @DisplayName("report treats an empty persistence result as a save failure")
    void reportTreatsEmptyPersistenceResultAsSaveFailure() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventAlarmService alarmService = mock(DqlEventAlarmService.class);
        DqlEventService service = new DqlEventService(eventRepository, alarmService);
        when(eventRepository.findDuplicate(eq(TASK_ID), any())).thenReturn(null);
        when(eventRepository.upsert(any(DqlEventDto.class))).thenReturn(null);

        BizException exception = assertThrows(BizException.class, () -> service.report(TASK_ID, reportVo()));

        assertEquals(BizException.SYSTEM_ERROR, exception.getErrorCode());
        assertNotNull(exception.getCause());
        verify(alarmService).notifySaveFailed(eq(TASK_ID), contains("returned no event"));
        verify(alarmService, never()).notifyEventCreated(any(DqlEventDto.class));
    }

    @Test
    @DisplayName("report preserves persistence error when save failure alarm throws and redacts sensitive details")
    void reportPreservesPersistenceErrorWhenSaveFailureAlarmThrows() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventAlarmService alarmService = mock(DqlEventAlarmService.class);
        DqlEventService service = new DqlEventService(eventRepository, alarmService);
        IllegalStateException cause = new IllegalStateException("payload token=must-not-leak");
        when(eventRepository.findDuplicate(eq(TASK_ID), any())).thenReturn(null);
        when(eventRepository.upsert(any(DqlEventDto.class))).thenThrow(cause);
        doThrow(new IllegalStateException("alarm unavailable"))
                .when(alarmService).notifySaveFailed(eq(TASK_ID), any(String.class));

        BizException exception = assertThrows(BizException.class, () -> service.report(TASK_ID, reportVo()));

        assertEquals(BizException.SYSTEM_ERROR, exception.getErrorCode());
        assertSame(cause, exception.getCause());
        ArgumentCaptor<String> reasonCaptor = forClass(String.class);
        verify(alarmService).notifySaveFailed(eq(TASK_ID), reasonCaptor.capture());
        assertEquals("IllegalStateException", reasonCaptor.getValue());
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
    @DisplayName("summary ignores status and pagination while preserving the other filters")
    void summaryIgnoresStatusAndPagination() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventService service = new DqlEventService(eventRepository, mock(DqlEventAlarmService.class));
        DqlEventQueryVo query = new DqlEventQueryVo();
        query.setTaskId(TASK_ID);
        query.setTaskName("sync_order");
        query.setStatus(DqlEventStatusEnum.RECOVERED.name());
        query.setSkip(40L);
        query.setLimit(20);
        query.setOrder("-eventTime");
        when(eventRepository.count(any(DqlEventQueryVo.class))).thenReturn(42L);
        when(eventRepository.countByStatus(any(DqlEventQueryVo.class), any())).thenReturn(1L);

        service.summary(query, user());

        ArgumentCaptor<DqlEventQueryVo> totalQuery = forClass(DqlEventQueryVo.class);
        verify(eventRepository).count(totalQuery.capture());
        assertSummaryQuery(totalQuery.getValue());
        ArgumentCaptor<DqlEventQueryVo> statusQuery = forClass(DqlEventQueryVo.class);
        verify(eventRepository, times(5)).countByStatus(statusQuery.capture(), any());
        statusQuery.getAllValues().forEach(this::assertSummaryQuery);
    }

    @Test
    @DisplayName("summary uses one task permission scope for total and every status count")
    void summaryUsesPermissionScopeForAllCounts() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventPermissionService permissionService = mock(DqlEventPermissionService.class);
        UserDetail user = user();
        DqlEventService service = new DqlEventService(
                eventRepository,
                mock(DqlEventAlarmService.class),
                permissionService
        );
        DqlEventQueryVo query = new DqlEventQueryVo();
        Set<String> visibleTaskIds = Set.of(TASK_ID);
        when(permissionService.resolveVisibleTaskIds(any(DqlEventQueryVo.class), eq(user))).thenReturn(visibleTaskIds);
        when(eventRepository.count(any(DqlEventQueryVo.class), eq(visibleTaskIds))).thenReturn(42L);
        when(eventRepository.countByStatus(any(DqlEventQueryVo.class), any(DqlEventStatusEnum.class), eq(visibleTaskIds)))
                .thenReturn(1L);

        DqlEventSummaryVo summary = service.summary(query, user);

        assertEquals(42L, summary.getTotal());
        verify(eventRepository).count(any(DqlEventQueryVo.class), eq(visibleTaskIds));
        verify(eventRepository, times(5)).countByStatus(any(DqlEventQueryVo.class), any(DqlEventStatusEnum.class), eq(visibleTaskIds));
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
    @DisplayName("preview checks every selected task before exposing cross-task data")
    void previewChecksEverySelectedTaskBeforeCrossTaskError() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventPermissionService permissionService = mock(DqlEventPermissionService.class);
        UserDetail user = user();
        DqlRecoveryBatchService service = new DqlRecoveryBatchService(
                eventRepository,
                mock(DqlRecoveryBatchRepository.class),
                permissionService,
                mock(MessageQueueServiceImpl.class),
                mock(DqlEventAlarmService.class)
        );
        String otherTaskId = "64f000000000000000000002";
        when(eventRepository.findByEventIds(List.of("DQL-1", "DQL-2")))
                .thenReturn(List.of(
                        event("DQL-1", TASK_ID, 2L, DqlEventStatusEnum.PENDING),
                        event("DQL-2", otherTaskId, 1L, DqlEventStatusEnum.PENDING)
                ));
        doAnswer(invocation -> {
            if (otherTaskId.equals(invocation.getArgument(0))) {
                throw new RuntimeException("NoPermission");
            }
            return null;
        }).when(permissionService).checkTaskVisible(any(String.class), eq(user));
        DqlRecoveryRequestVo request = new DqlRecoveryRequestVo();
        request.setEventIds(List.of("DQL-1", "DQL-2"));

        RuntimeException exception = assertThrows(RuntimeException.class, () -> service.preview(request, user));

        assertEquals("NoPermission", exception.getMessage());
        verify(permissionService).checkTaskVisible(eq(TASK_ID), eq(user));
        verify(permissionService).checkTaskVisible(eq(otherTaskId), eq(user));
    }

    @Test
    @DisplayName("detail checks menu permission before looking up an event")
    void detailChecksMenuPermissionBeforeLookup() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventPermissionService permissionService = mock(DqlEventPermissionService.class);
        UserDetail user = user();
        doThrow(new RuntimeException("NoPermission")).when(permissionService).checkMenuVisible(user);
        DqlEventService service = new DqlEventService(
                eventRepository,
                mock(DqlEventAlarmService.class),
                permissionService
        );

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> service.detail("DQL-hidden", user));

        assertEquals("NoPermission", exception.getMessage());
        verify(eventRepository, never()).findByEventId(any(String.class));
    }

    @Test
    @DisplayName("detail checks the event task permission after lookup")
    void detailChecksEventTaskPermission() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventPermissionService permissionService = mock(DqlEventPermissionService.class);
        UserDetail user = user();
        DqlEventDto event = event("DQL-hidden", TASK_ID, 1L, DqlEventStatusEnum.PENDING);
        when(eventRepository.findByEventId("DQL-hidden")).thenReturn(event);
        doThrow(new RuntimeException("NoPermission")).when(permissionService).checkTaskVisible(TASK_ID, user);
        DqlEventService service = new DqlEventService(
                eventRepository,
                mock(DqlEventAlarmService.class),
                permissionService
        );

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> service.detail("DQL-hidden", user));

        assertEquals("NoPermission", exception.getMessage());
        verify(permissionService).checkMenuVisible(user);
        verify(permissionService).checkTaskVisible(TASK_ID, user);
    }

    @Test
    @DisplayName("recovery preview checks menu permission before looking up selected events")
    void recoveryPreviewChecksMenuPermissionBeforeLookup() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventPermissionService permissionService = mock(DqlEventPermissionService.class);
        UserDetail user = user();
        doThrow(new RuntimeException("NoPermission")).when(permissionService).checkMenuVisible(user);
        DqlRecoveryBatchService service = new DqlRecoveryBatchService(
                eventRepository,
                mock(DqlRecoveryBatchRepository.class),
                permissionService,
                mock(MessageQueueServiceImpl.class),
                mock(DqlEventAlarmService.class)
        );
        DqlRecoveryRequestVo request = new DqlRecoveryRequestVo();
        request.setEventIds(List.of("DQL-hidden"));

        RuntimeException exception = assertThrows(RuntimeException.class, () -> service.preview(request, user));

        assertEquals("NoPermission", exception.getMessage());
        verify(eventRepository, never()).findByEventIds(any());
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
        batch.setStatus(DqlRecoveryBatchStatusEnum.RUNNING.name());
        when(batchRepository.findByBatchId("DQLB-20260825-000001")).thenReturn(batch);
        when(eventRepository.completeEventIdempotent(eq("DQL-64f000-000001"), eq("DQLB-20260825-000001"), any()))
                .thenReturn(DqlRecoveryCallbackResultEnum.APPLIED);
        DqlRecoveryResultReportVo report = new DqlRecoveryResultReportVo();
        report.setBatchId("DQLB-20260825-000001");
        report.setEventId("DQL-64f000-000001");
        report.setAttemptId("A-000001");
        report.setType("EVENT_RESULT");
        report.setResult(DqlRecoveryAttemptResultEnum.SUCCESS.name());

        service.report(TASK_ID, report);

        verify(eventRepository).completeEventIdempotent(eq("DQL-64f000-000001"), eq("DQLB-20260825-000001"), any());
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
    @DisplayName("record success report returns an unmarked result without creating an event when no prior event matches")
    void recordSuccessReportReturnsUnmarkedWhenNoEventMatches() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventService service = new DqlEventService(eventRepository, mock(DqlEventAlarmService.class));
        DqlRecordSuccessReportVo report = recordSuccessReport();
        when(eventRepository.markLaterSuccess(eq(TASK_ID), eq(report), any())).thenReturn(null);

        DqlRecordSuccessReportResultVo result = service.reportRecordSuccess(TASK_ID, report);

        assertFalse(result.isMarked());
        assertNull(result.getEventId());
        assertEquals("key:orders:id=1001", result.getRecordIdentity());
        assertNull(result.getOverwriteRiskMessage());
        verify(eventRepository).markLaterSuccess(eq(TASK_ID), eq(report), any());
    }

    @Test
    @DisplayName("page delegates query to repository and preserves pagination result")
    void pageDelegatesToRepository() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventService service = new DqlEventService(eventRepository, mock(DqlEventAlarmService.class));
        DqlEventQueryVo query = new DqlEventQueryVo();
        Page<DqlEventDto> page = Page.page(List.of(event("DQL-64f000-000001", TASK_ID, 1L, DqlEventStatusEnum.PENDING)), 1);
        when(eventRepository.page(query)).thenReturn(page);

        Page<DqlEventListVo> result = service.page(query, user());
        assertEquals(page.getTotal(), result.getTotal());
        assertEquals(page.getItems().get(0).getEventId(), result.getItems().get(0).getEventId());
    }

    @Test
    @DisplayName("page passes only the user's visible task range to the repository")
    void pageUsesVisibleTaskRange() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        TaskService taskService = mock(TaskService.class);
        UserDetail user = user();
        TaskEntity visibleTask = new TaskEntity();
        visibleTask.setId(new ObjectId(TASK_ID));
        when(taskService.findAll(any(Query.class), eq(user))).thenReturn(List.of(visibleTask));
        DqlEventPermissionService permissionService = new DqlEventPermissionService(taskService) {
            @Override
            public void checkMenuVisible(UserDetail ignored) {
            }

            @Override
            public void checkTaskVisible(String ignored, UserDetail ignoredUser) {
            }
        };
        DqlEventService service = new DqlEventService(eventRepository, mock(DqlEventAlarmService.class), permissionService);
        DqlEventQueryVo query = new DqlEventQueryVo();
        when(eventRepository.page(eq(query), eq(Set.of(TASK_ID)))).thenReturn(Page.empty());

        Page<DqlEventListVo> result = service.page(query, user);

        assertTrue(result.getItems().isEmpty());
        assertEquals(0L, result.getTotal());
        verify(taskService).findAll(any(Query.class), eq(user));
        verify(eventRepository).page(eq(query), eq(Set.of(TASK_ID)));
        verify(eventRepository, never()).page(eq(query));
    }

    @Test
    @DisplayName("page response omits full payload and attempt history from list items")
    void pageOmitsPayloadAndAttempts() throws Exception {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventService service = new DqlEventService(eventRepository, mock(DqlEventAlarmService.class));
        DqlEventQueryVo query = new DqlEventQueryVo();
        DqlEventDto event = event("DQL-64f000-000001", TASK_ID, 1L, DqlEventStatusEnum.PENDING);
        event.setFailedAt(new Date(2000));
        event.setPayloadData(Map.of("after", Map.of("id", 1001)));
        event.setRecoveryAttempts(List.of(new com.tapdata.tm.dql.dto.DqlRecoveryAttemptDto()));
        when(eventRepository.page(query)).thenReturn(Page.page(List.of(event), 1));

        String json = new ObjectMapper().writeValueAsString(service.page(query, user()));

        assertFalse(json.contains("\"payloadData\""));
        assertFalse(json.contains("\"recoveryAttempts\""));
    }

    @Test
    @DisplayName("detail response includes current batch for reprocessing events")
    void detailIncludesCurrentBatchForReprocessingEvent() throws Exception {
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

        String json = new ObjectMapper().writeValueAsString(detail);
        assertFalse(json.contains("\"payloadData\""));
        assertNotNull(detail.getCurrentBatch());
        assertEquals("DQLB-20260826-000001", detail.getCurrentBatch().getBatchId());
        assertEquals(DqlRecoveryBatchStatusEnum.RUNNING.name(), detail.getCurrentBatch().getStatus());
    }

    @Test
    @DisplayName("recovery payload contains only the immutable fields needed by Engine")
    void recoveryPayloadMapsImmutableFields() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventService service = new DqlEventService(eventRepository, mock(DqlEventAlarmService.class));
        DqlEventDto event = event("DQL-64f000-000001", TASK_ID, 1L, DqlEventStatusEnum.PENDING);
        event.setPayloadFormat("tap-record-event-json-v1");
        event.setPayloadData(Map.of("after", Map.of("id", 1001)));
        event.setPayloadHash("sha256:payload");
        event.setPayloadSize(128L);
        event.setPayloadComplete(true);
        event.setPayloadPreview(Map.of("id", 1001));
        event.setPayloadPreviewTruncated(false);
        when(eventRepository.findByEventId("DQL-64f000-000001")).thenReturn(event);

        DqlRecoveryPayloadVo payload = service.recoveryPayload("DQL-64f000-000001", user());

        assertEquals(event.getPayloadFormat(), payload.getPayloadFormat());
        assertEquals(event.getPayloadData(), payload.getPayloadData());
        assertEquals(event.getPayloadHash(), payload.getPayloadHash());
        assertEquals(event.getPayloadSize(), payload.getPayloadSize());
        assertEquals(event.getPayloadComplete(), payload.getPayloadComplete());
        assertEquals(event.getPayloadPreview(), payload.getPayloadPreview());
        assertEquals(event.getPayloadPreviewTruncated(), payload.getPayloadPreviewTruncated());
    }

    @Test
    @DisplayName("batch detail checks menu permission before looking up a batch")
    void batchDetailChecksMenuPermissionBeforeLookup() {
        DqlEventPermissionService permissionService = mock(DqlEventPermissionService.class);
        UserDetail user = user();
        doThrow(new RuntimeException("NoPermission")).when(permissionService).checkMenuVisible(user);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        DqlRecoveryBatchService service = new DqlRecoveryBatchService(
                mock(DqlEventRepository.class),
                batchRepository,
                permissionService,
                mock(MessageQueueServiceImpl.class),
                mock(DqlEventAlarmService.class)
        );

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> service.detail("DQLB-hidden", user));

        assertEquals("NoPermission", exception.getMessage());
        verify(batchRepository, never()).findByBatchId(any(String.class));
    }

    @Test
    @DisplayName("batch detail checks the batch task permission after lookup")
    void batchDetailChecksTaskPermission() {
        DqlEventPermissionService permissionService = mock(DqlEventPermissionService.class);
        DqlRecoveryBatchRepository batchRepository = mock(DqlRecoveryBatchRepository.class);
        UserDetail user = user();
        DqlRecoveryBatchDto batch = new DqlRecoveryBatchDto();
        batch.setBatchId("DQLB-hidden");
        batch.setTaskId(TASK_ID);
        when(batchRepository.findByBatchId("DQLB-hidden")).thenReturn(batch);
        doThrow(new RuntimeException("NoPermission")).when(permissionService).checkTaskVisible(TASK_ID, user);
        DqlRecoveryBatchService service = new DqlRecoveryBatchService(
                mock(DqlEventRepository.class),
                batchRepository,
                permissionService,
                mock(MessageQueueServiceImpl.class),
                mock(DqlEventAlarmService.class)
        );

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> service.detail("DQLB-hidden", user));

        assertEquals("NoPermission", exception.getMessage());
        verify(permissionService).checkMenuVisible(user);
        verify(permissionService).checkTaskVisible(TASK_ID, user);
    }

    @Test
    @DisplayName("detail exposes only safe frontend fields and maps internal names")
    void detailExposesSafeFrontendFields() throws Exception {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventService service = new DqlEventService(eventRepository, mock(DqlEventAlarmService.class));
        DqlEventDto event = event("DQL-64f000-000001", TASK_ID, 1L, DqlEventStatusEnum.REPROCESSING);
        event.setFailedStage("TRANSFORM");
        event.setRecoveryCount(2);
        event.setEventKey(new LinkedHashMap<>(Map.of("id", 1001, "password", "must-not-leak")));
        event.setPayloadData(Map.of("after", Map.of("id", 1001)));
        DqlRecoveryAttemptDto attempt = new DqlRecoveryAttemptDto();
        attempt.setAttemptId("A-000001");
        attempt.setResult(DqlRecoveryAttemptResultEnum.RUNNING.name());
        attempt.setErrorDetails("internal details");
        event.setRecoveryAttempts(List.of(attempt));
        when(eventRepository.findByEventId(event.getEventId())).thenReturn(event);

        DqlEventDetailVo detail = service.detail(event.getEventId(), user());
        String json = new ObjectMapper().writeValueAsString(detail);

        assertEquals("{\"id\":1001,\"password\":\"******\"}", detail.getEventKey());
        assertEquals(2, detail.getRecoveryCount());
        assertTrue(json.contains("\"stage\":\"TRANSFORM\""));
        assertTrue(json.contains("\"errorMessage\":\"internal details\""));
        assertFalse(json.contains("\"failedStage\""));
        assertFalse(json.contains("\"payloadData\""));
        assertFalse(json.contains("\"errorDetails\""));
    }

    @Test
    @DisplayName("detail returns at most twenty attempts in most recent first order")
    void detailReturnsRecentAttemptsFirst() {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventService service = new DqlEventService(eventRepository, mock(DqlEventAlarmService.class));
        DqlEventDto event = event("DQL-64f000-000001", TASK_ID, 1L, DqlEventStatusEnum.REPROCESSING);
        List<DqlRecoveryAttemptDto> attempts = new java.util.ArrayList<>();
        for (int i = 1; i <= 21; i++) {
            DqlRecoveryAttemptDto attempt = new DqlRecoveryAttemptDto();
            attempt.setAttemptId("A-" + String.format("%06d", i));
            attempt.setResult(i == 21 ? DqlRecoveryAttemptResultEnum.RUNNING.name() : DqlRecoveryAttemptResultEnum.FAILED.name());
            attempts.add(attempt);
        }
        event.setRecoveryAttempts(attempts);
        when(eventRepository.findByEventId(event.getEventId())).thenReturn(event);

        DqlEventDetailVo detail = service.detail(event.getEventId(), user());

        assertEquals(20, detail.getRecoveryAttempts().size());
        assertEquals("A-000021", detail.getRecoveryAttempts().get(0).getAttemptId());
        assertEquals(DqlRecoveryAttemptResultEnum.RUNNING.name(), detail.getRecoveryAttempts().get(0).getResult());
        assertEquals("A-000002", detail.getRecoveryAttempts().get(19).getAttemptId());
    }

    @Test
    @DisplayName("page exposes only list fields and never serializes payload or internal audit fields")
    void pageExposesOnlyListFields() throws Exception {
        DqlEventRepository eventRepository = mock(DqlEventRepository.class);
        DqlEventService service = new DqlEventService(eventRepository, mock(DqlEventAlarmService.class));
        DqlEventDto event = event("DQL-64f000-000001", TASK_ID, 1L, DqlEventStatusEnum.PENDING);
        event.setFailedAt(new Date(2000));
        event.setPayloadData(Map.of("after", Map.of("id", 1001)));
        event.setOverwriteRisk(true);
        event.setRecordIdentity("key:orders:id=1001");
        when(eventRepository.page(any(DqlEventQueryVo.class))).thenReturn(Page.page(List.of(event), 1));

        String json = new ObjectMapper().writeValueAsString(service.page(new DqlEventQueryVo(), user()));

        assertTrue(json.contains("\"taskName\""));
        assertTrue(json.contains("\"failedAt\""));
        assertFalse(json.contains("\"payloadData\""));
        assertFalse(json.contains("\"overwriteRisk\""));
        assertFalse(json.contains("\"recordIdentity\""));
    }

    private void assertSummaryQuery(DqlEventQueryVo summaryQuery) {
        assertEquals(TASK_ID, summaryQuery.getTaskId());
        assertEquals("sync_order", summaryQuery.getTaskName());
        assertNull(summaryQuery.getStatus());
        assertEquals(0L, summaryQuery.getSkip());
        assertEquals(0, summaryQuery.getLimit());
        assertNull(summaryQuery.getOrder());
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
