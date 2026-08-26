package com.tapdata.tm.dql.controller;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dql.DqlEventStatusEnum;
import com.tapdata.tm.dql.dto.DqlEventDto;
import com.tapdata.tm.dql.dto.DqlRecoveryBatchDto;
import com.tapdata.tm.dql.service.DqlEventService;
import com.tapdata.tm.dql.service.DqlRecoveryBatchService;
import com.tapdata.tm.dql.vo.DqlEventDetailVo;
import com.tapdata.tm.dql.vo.DqlEventQueryVo;
import com.tapdata.tm.dql.vo.DqlEventReportResultVo;
import com.tapdata.tm.dql.vo.DqlEventReportVo;
import com.tapdata.tm.dql.vo.DqlEventSummaryVo;
import com.tapdata.tm.dql.vo.DqlRecoveryPreviewVo;
import com.tapdata.tm.dql.vo.DqlRecoveryRequestVo;
import com.tapdata.tm.dql.vo.DqlRecoveryResultReportVo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DqlEventControllerTest {

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
    @DisplayName("page maps query parameters into DqlEventQueryVo")
    void pageMapsQueryParameters() {
        DqlEventService eventService = mock(DqlEventService.class);
        DqlEventController controller = spy(new DqlEventController(eventService, mock(DqlRecoveryBatchService.class)));
        UserDetail user = user();
        doReturn(user).when(controller).getLoginUser();
        Page<DqlEventDto> page = Page.page(List.of(new DqlEventDto()), 1);
        when(eventService.page(org.mockito.ArgumentMatchers.any(DqlEventQueryVo.class), eq(user))).thenReturn(page);

        ResponseMessage<Page<DqlEventDto>> response = controller.page(
                "64f000000000000000000001",
                "sync",
                "orders",
                "orders_sink",
                "JS_PROCESS_FAILED",
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
        assertEquals("sync", query.getTaskName());
        assertEquals("orders", query.getSourceTable());
        assertEquals("orders_sink", query.getTargetTable());
        assertEquals("JS_PROCESS_FAILED", query.getKeyword());
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
                null, null, null, null, null, null, null, "RECOVERY_FAILED", null, null
        );

        ArgumentCaptor<DqlEventQueryVo> captor = ArgumentCaptor.forClass(DqlEventQueryVo.class);
        verify(eventService).summary(captor.capture(), eq(user));
        assertSame(summary, response.getData());
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
