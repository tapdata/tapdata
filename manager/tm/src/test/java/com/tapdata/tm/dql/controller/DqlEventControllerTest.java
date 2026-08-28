package com.tapdata.tm.dql.controller;

import com.tapdata.tm.base.annotation.IgnoreRequestBodyLog;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dql.DqlEventStatusEnum;
import com.tapdata.tm.dql.dto.DqlRecoveryBatchDto;
import com.tapdata.tm.dql.service.DqlEventService;
import com.tapdata.tm.dql.service.DqlRecoveryBatchService;
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DqlEventControllerTest {

    @Test
    @DisplayName("engine callback endpoints suppress generic request body logging")
    void engineCallbacksSuppressRequestBodyLogging() throws Exception {
        assertTrue(DqlEventController.class
                .getMethod("report", String.class, DqlEventReportVo.class)
                .isAnnotationPresent(IgnoreRequestBodyLog.class));
        assertTrue(DqlEventController.class
                .getMethod("reportRecordSuccess", String.class, DqlRecordSuccessReportVo.class)
                .isAnnotationPresent(IgnoreRequestBodyLog.class));
        assertTrue(DqlEventController.class
                .getMethod("reportRecovery", String.class, DqlRecoveryResultReportVo.class)
                .isAnnotationPresent(IgnoreRequestBodyLog.class));
    }

    @Test
    @DisplayName("engine callback mappings expose canonical lowercase and legacy uppercase task paths")
    void engineCallbackMappingsExposeCanonicalAndLegacyPaths() throws Exception {
        assertArrayEquals(
                new String[]{"/api/task/{taskId}/dql-events/report", "/api/Task/{taskId}/dql-events/report"},
                DqlEventController.class.getMethod("report", String.class, DqlEventReportVo.class)
                        .getAnnotation(PostMapping.class).value());
        assertArrayEquals(
                new String[]{"/api/task/{taskId}/dql-events/record-success/report", "/api/Task/{taskId}/dql-events/record-success/report"},
                DqlEventController.class.getMethod("reportRecordSuccess", String.class, DqlRecordSuccessReportVo.class)
                        .getAnnotation(PostMapping.class).value());
    }

    @Test
    @DisplayName("report uses task id from path instead of request body")
    void reportUsesPathTaskId() {
        DqlEventService eventService = mock(DqlEventService.class);
        DqlEventController controller = new DqlEventController(eventService, mock(DqlRecoveryBatchService.class));
        DqlEventReportVo request = new DqlEventReportVo();
        request.setTaskName("sync_order");
        DqlEventReportResultVo result = new DqlEventReportResultVo();
        result.setEventId("DQL-64f000-000001");
        result.setStatus(DqlEventStatusEnum.PENDING.name());
        when(eventService.report("64f000000000000000000001", request)).thenReturn(result);

        ResponseMessage<DqlEventReportResultVo> response = controller.report("64f000000000000000000001", request);

        assertSame(result, response.getData());
        verify(eventService).report("64f000000000000000000001", request);
    }

    @Test
    @DisplayName("record success report uses task id from path")
    void recordSuccessReportUsesPathTaskId() {
        DqlEventService eventService = mock(DqlEventService.class);
        DqlEventController controller = new DqlEventController(eventService, mock(DqlRecoveryBatchService.class));
        DqlRecordSuccessReportVo request = new DqlRecordSuccessReportVo();
        request.setRecordIdentity("key:orders:id=1001");
        DqlRecordSuccessReportResultVo result = new DqlRecordSuccessReportResultVo();
        result.setEventId("DQL-64f000-000001");
        result.setMarked(true);
        when(eventService.reportRecordSuccess("64f000000000000000000001", request)).thenReturn(result);

        ResponseMessage<DqlRecordSuccessReportResultVo> response = controller.reportRecordSuccess("64f000000000000000000001", request);

        assertSame(result, response.getData());
        verify(eventService).reportRecordSuccess("64f000000000000000000001", request);
    }

    @Test
    @DisplayName("recovery payload endpoint uses the dedicated event payload service")
    void recoveryPayloadUsesDedicatedService() {
        DqlEventService eventService = mock(DqlEventService.class);
        DqlEventController controller = spy(new DqlEventController(eventService, mock(DqlRecoveryBatchService.class)));
        UserDetail user = user();
        doReturn(user).when(controller).getLoginUser();
        DqlRecoveryPayloadVo payload = new DqlRecoveryPayloadVo();
        payload.setPayloadFormat("tap-record-event-json-v1");
        when(eventService.recoveryPayload("DQL-1", user)).thenReturn(payload);

        ResponseMessage<DqlRecoveryPayloadVo> response = controller.recoveryPayload("DQL-1");

        assertSame(payload, response.getData());
        verify(eventService).recoveryPayload("DQL-1", user);
    }

    @Test
    @DisplayName("page maps query parameters into DqlEventQueryVo")
    void pageMapsQueryParameters() {
        DqlEventService eventService = mock(DqlEventService.class);
        DqlEventController controller = spy(new DqlEventController(eventService, mock(DqlRecoveryBatchService.class)));
        UserDetail user = user();
        doReturn(user).when(controller).getLoginUser();
        Page<DqlEventListVo> page = Page.page(List.of(new DqlEventListVo()), 1);
        when(eventService.page(org.mockito.ArgumentMatchers.any(DqlEventQueryVo.class), eq(user))).thenReturn(page);

        ResponseMessage<Page<DqlEventListVo>> response = controller.page(
                "64f000000000000000000001",
                "event-1",
                "sync",
                "orders",
                "orders_sink",
                "JS_PROCESS_FAILED",
                "DUPLICATE_KEY",
                "U",
                "TRANSFORM_ERROR",
                "PENDING",
                1000L,
                2000L,
                5L,
                20,
                "-failedAt"
        );

        ArgumentCaptor<DqlEventQueryVo> captor = ArgumentCaptor.forClass(DqlEventQueryVo.class);
        verify(eventService).page(captor.capture(), eq(user));
        DqlEventQueryVo query = captor.getValue();
        assertSame(page, response.getData());
        assertEquals("64f000000000000000000001", query.getTaskId());
        assertEquals("event-1", query.getEventId());
        assertEquals("sync", query.getTaskName());
        assertEquals("orders", query.getSourceTable());
        assertEquals("orders_sink", query.getTargetTable());
        assertEquals("JS_PROCESS_FAILED", query.getKeyword());
        assertEquals("DUPLICATE_KEY", query.getErrorCode());
        assertEquals("U", query.getDmlType());
        assertEquals("TRANSFORM_ERROR", query.getErrorType());
        assertEquals("PENDING", query.getStatus());
        assertEquals(new Date(1000L), query.getStartTime());
        assertEquals(new Date(2000L), query.getEndTime());
        assertEquals(5L, query.getSkip());
        assertEquals(20, query.getLimit());
        assertEquals("-failedAt", query.getOrder());
    }

    @Test
    @DisplayName("page endpoint defaults to twenty items for the frontend contract")
    void pageDefaultsToTwentyItems() throws Exception {
        RequestParam limit = java.util.Arrays.stream(DqlEventController.class
                        .getMethod("page", String.class, String.class, String.class, String.class, String.class,
                                String.class, String.class, String.class, String.class, String.class,
                                Long.class, Long.class, long.class, int.class, String.class)
                        .getParameterAnnotations()[13])
                .filter(RequestParam.class::isInstance)
                .map(RequestParam.class::cast)
                .findFirst()
                .orElseThrow();

        assertEquals("limit", limit.name());
        assertEquals("20", limit.defaultValue());
    }

    @Test
    @DisplayName("detail delegates event id and current user to service")
    void detailDelegatesToService() {
        DqlEventService eventService = mock(DqlEventService.class);
        DqlEventController controller = spy(new DqlEventController(eventService, mock(DqlRecoveryBatchService.class)));
        UserDetail user = user();
        doReturn(user).when(controller).getLoginUser();
        DqlEventDetailVo detail = new DqlEventDetailVo();
        when(eventService.detail("DQL-64f000-000001", user)).thenReturn(detail);

        ResponseMessage<DqlEventDetailVo> response = controller.detail("DQL-64f000-000001");

        assertSame(detail, response.getData());
        verify(eventService).detail("DQL-64f000-000001", user);
    }

    @Test
    @DisplayName("summary reuses query parameter mapping")
    void summaryMapsQueryParameters() {
        DqlEventService eventService = mock(DqlEventService.class);
        DqlEventController controller = spy(new DqlEventController(eventService, mock(DqlRecoveryBatchService.class)));
        UserDetail user = user();
        doReturn(user).when(controller).getLoginUser();
        DqlEventSummaryVo summary = new DqlEventSummaryVo();
        when(eventService.summary(org.mockito.ArgumentMatchers.any(DqlEventQueryVo.class), eq(user))).thenReturn(summary);

        ResponseMessage<DqlEventSummaryVo> response = controller.summary(
                null, "event-1", null, null, null, null, null, null, null, "RECOVERY_FAILED", null, null
        );

        ArgumentCaptor<DqlEventQueryVo> captor = ArgumentCaptor.forClass(DqlEventQueryVo.class);
        verify(eventService).summary(captor.capture(), eq(user));
        assertSame(summary, response.getData());
        assertEquals("event-1", captor.getValue().getEventId());
        assertEquals("RECOVERY_FAILED", captor.getValue().getStatus());
        assertEquals(0L, captor.getValue().getSkip());
        assertEquals(0, captor.getValue().getLimit());
    }

    @Test
    @DisplayName("recovery preview delegates request and user to recovery service")
    void recoveryPreviewDelegates() {
        DqlRecoveryBatchService recoveryService = mock(DqlRecoveryBatchService.class);
        DqlEventController controller = spy(new DqlEventController(mock(DqlEventService.class), recoveryService));
        UserDetail user = user();
        doReturn(user).when(controller).getLoginUser();
        DqlRecoveryRequestVo request = new DqlRecoveryRequestVo();
        DqlRecoveryPreviewVo preview = new DqlRecoveryPreviewVo();
        when(recoveryService.preview(request, user)).thenReturn(preview);

        ResponseMessage<DqlRecoveryPreviewVo> response = controller.previewRecovery(request);

        assertSame(preview, response.getData());
        verify(recoveryService).preview(request, user);
    }

    @Test
    @DisplayName("engine recovery report uses task id from path")
    void engineRecoveryReportUsesPathTaskId() {
        DqlRecoveryBatchService recoveryService = mock(DqlRecoveryBatchService.class);
        DqlEventController controller = new DqlEventController(mock(DqlEventService.class), recoveryService);
        DqlRecoveryResultReportVo report = new DqlRecoveryResultReportVo();
        report.setBatchId("DQLB-20260825-000001");

        ResponseMessage<Boolean> response = controller.reportRecovery("64f000000000000000000001", report);

        assertEquals(Boolean.TRUE, response.getData());
        verify(recoveryService).report("64f000000000000000000001", report);
    }

    @Test
    @DisplayName("batch detail endpoint returns recovery batch by id")
    void batchDetailReturnsBatch() {
        DqlRecoveryBatchService recoveryService = mock(DqlRecoveryBatchService.class);
        DqlEventController controller = spy(new DqlEventController(mock(DqlEventService.class), recoveryService));
        UserDetail user = user();
        doReturn(user).when(controller).getLoginUser();
        DqlRecoveryBatchDto batch = new DqlRecoveryBatchDto();
        when(recoveryService.detail("DQLB-20260825-000001", user)).thenReturn(batch);

        ResponseMessage<DqlRecoveryBatchDto> response = controller.batchDetail("DQLB-20260825-000001");

        assertSame(batch, response.getData());
        verify(recoveryService).detail("DQLB-20260825-000001", user);
    }

    private static UserDetail user() {
        return new UserDetail("user-id", "customer-id", "Harsen", "password", Collections.<SimpleGrantedAuthority>emptyList());
    }
}
