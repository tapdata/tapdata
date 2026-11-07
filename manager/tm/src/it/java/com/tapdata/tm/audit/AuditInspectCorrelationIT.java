package com.tapdata.tm.audit;

import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.controller.InspectController;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectResultService;
import com.tapdata.tm.inspect.service.InspectService;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AuditInspectCorrelationIT {
    private static final String ID = "6aec62521fba1c750de5c933";
    private final AuditItFixture fixture = new AuditItFixture();
    private final InspectService service = mock(InspectService.class);
    private final InspectController controller = spy(new InspectController(mock(InspectResultService.class)));

    AuditInspectCorrelationIT() {
        controller.setInspectService(service);
        controller.setAuditOperationFinisher(fixture.finalizer);
        doReturn(AuditItFixture.user("u1", "ops@example.com", "username_login")).when(controller).getLoginUser();
        when(service.updateByWhere(any(Where.class), any(InspectDto.class), any(UserDetail.class))).thenReturn(1L);
    }

    @Test
    void stoppedReportFinishesOnlyTheMatchingStartAndStopOnce() {
        fixture.registry.register("inspect", ID, "start-1", AuditAction.INSPECT_START, "inspect");
        fixture.registry.register("inspect", ID, "stop-1", AuditAction.INSPECT_STOP, "inspect");
        fixture.registry.register("inspect", ID, "stop-2", AuditAction.INSPECT_STOP, "inspect");
        when(service.findOne(any(Query.class))).thenReturn(new InspectDto());
        InspectDto report = report("start-1", "stop-1", "stopped");
        controller.upsertByWhere("{\"id\":\"" + ID + "\"}", report);
        controller.upsertByWhere("{\"id\":\"" + ID + "\"}", report);
        assertThat(fixture.events).filteredOn(e -> "FINAL".equals(e.getPhase()))
                .extracting(e -> e.getOperationId()).containsExactlyInAnyOrder("start-1", "stop-1");
        assertThat(fixture.registry.pendingOperations("inspect", ID))
                .extracting(AuditOperationRegistry.Pending::getOperationId).containsExactly("stop-2");
        verify(service, times(2)).updateByWhere(argThat((Where where) -> "start-1".equals(where.get("auditRunOperationId"))),
                any(InspectDto.class), any(UserDetail.class));
    }

    @Test
    void staleReportCannotFinishTheCurrentRun() {
        fixture.registry.register("inspect", ID, "start-2", AuditAction.INSPECT_START, "inspect");
        when(service.findOne(any(Query.class))).thenReturn(null);
        controller.upsertByWhere("{\"id\":\"" + ID + "\"}", report("start-1", "stop-1", "stopped"));
        assertThat(fixture.events).noneMatch(e -> "FINAL".equals(e.getPhase()));
        assertThat(fixture.registry.pendingOperations("inspect", ID)).hasSize(1);
    }

    @Test
    void failedStopDoesNotFinishTheStillRunningStart() {
        fixture.registry.register("inspect", ID, "start-1", AuditAction.INSPECT_START, "inspect");
        fixture.registry.register("inspect", ID, "stop-1", AuditAction.INSPECT_STOP, "inspect");
        when(service.findOne(any(Query.class))).thenReturn(new InspectDto());
        controller.upsertByWhere("{\"id\":\"" + ID + "\"}", report("start-1", "stop-1", "stop_failed"));
        assertThat(fixture.events).filteredOn(e -> "FINAL".equals(e.getPhase()))
                .singleElement().satisfies(event -> {
                    assertThat(event.getOperationId()).isEqualTo("stop-1");
                    assertThat(event.getOutcome()).isEqualTo("FAILED");
                });
        assertThat(fixture.registry.pendingOperations("inspect", ID))
                .extracting(AuditOperationRegistry.Pending::getOperationId).containsExactly("start-1");
    }

    @Test
    void correlatedReportRequiresAnExactInspectId() {
        assertThatThrownBy(() -> controller.upsertByWhere("{}", report("start-1", "stop-1", "stopped")))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(service);
    }

    @Test
    void rejectedStopDoesNotChangeTheBusinessStatus() {
        fixture.registry.register("inspect", ID, "stop-1", AuditAction.INSPECT_STOP, "inspect");
        when(service.findOne(any(Query.class))).thenReturn(new InspectDto());
        controller.upsertByWhere("{\"id\":\"" + ID + "\"}", report("start-1", "stop-1", "stop_rejected"));
        verify(service, never()).updateByWhere(any(Where.class), any(InspectDto.class), any(UserDetail.class));
        assertThat(fixture.events).filteredOn(e -> "FINAL".equals(e.getPhase()))
                .singleElement().satisfies(event -> assertThat(event.getOutcome()).isEqualTo("FAILED"));
    }

    private static InspectDto report(String runId, String stopId, String completion) {
        InspectDto dto = new InspectDto();
        dto.setAuditRunOperationId(runId);
        dto.setAuditStopOperationId(stopId);
        dto.setAuditCompletion(completion);
        dto.setStatus("error");
        return dto;
    }
}
