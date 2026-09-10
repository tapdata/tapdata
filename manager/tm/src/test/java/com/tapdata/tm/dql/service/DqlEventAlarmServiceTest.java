package com.tapdata.tm.dql.service;

import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.alarm.AlarmComponentEnum;
import com.tapdata.tm.commons.alarm.AlarmStatusEnum;
import com.tapdata.tm.commons.alarm.AlarmTypeEnum;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.dql.dto.DqlEventDto;
import com.tapdata.tm.dql.dto.DqlRecoveryBatchDto;
import com.tapdata.tm.dql.vo.DqlStormGuardReportVo;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class DqlEventAlarmServiceTest {

    @Test
    void savesSafeAlarmForCreatedEvent() {
        List<AlarmInfo> saved = new ArrayList<>();
        DqlEventAlarmService service = service(saved, false);

        DqlEventDto event = new DqlEventDto();
        event.setTaskId("task-1");
        event.setTaskName("Orders");
        event.setAgentId("agent-1");
        event.setEventId("event-1");
        event.setSourceTable("orders");
        event.setTargetTable("warehouse.orders");
        event.setDmlType("UPDATE");
        event.setErrorType("TARGET_WRITE");
        event.setErrorCode("DQL-1001");
        event.setClassificationReason("target write rejected");
        event.setErrorDetails("target write rejected: password=should-not-be-mailed");
        event.setFailedAt(new Date(1_700_000_000_000L));
        event.setPayloadData(Map.of("password", "must-not-enter-alarm"));

        service.notifyEventCreated(event);

        assertEquals(1, saved.size());
        AlarmInfo alarm = saved.get(0);
        assertEquals(AlarmKeyEnum.TASK_DQL_EVENT, alarm.getMetric());
        assertEquals(AlarmStatusEnum.ING, alarm.getStatus());
        assertEquals(AlarmComponentEnum.FE, alarm.getComponent());
        assertEquals(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM, alarm.getType());
        assertEquals("task-1", alarm.getTaskId());
        assertEquals("Orders", alarm.getName());
        assertEquals("event-1", alarm.getParam().get("eventId"));
        assertEquals("[redacted]", alarm.getParam().get("safeReason"));
        assertEquals("target write rejected", alarm.getParam().get("classificationReason"));
        assertEquals(0L, alarm.getParam().get("pendingCount"));
        assertEquals("/exception-events?taskId=task-1&eventId=event-1", alarm.getParam().get("pageUrl"));
        assertFalse(alarm.getParam().containsKey("payloadData"));
        assertFalse(alarm.getParam().containsKey("payload_data"));
    }

    @Test
    void savesRecoveryAlarmAndDoesNotPropagateAlarmBackendFailure() {
        List<AlarmInfo> saved = new ArrayList<>();
        DqlEventAlarmService service = service(saved, true);
        DqlRecoveryBatchDto batch = new DqlRecoveryBatchDto();
        batch.setBatchId("batch-1");
        batch.setTaskId("task-1");
        batch.setTaskName("Orders");
        batch.setOperatorName("operator");
        batch.setStatus("PARTIAL_FAILED");
        batch.setSelectedCount(3);
        batch.setSuccessCount(1);
        batch.setFailedCount(1);
        batch.setSkippedCount(1);

        assertDoesNotThrow(() -> service.notifyBatchPartialFailed(batch));
        assertDoesNotThrow(() -> service.notifyRecoveryFailed(batch));
        assertNotNull(batch);
        assertEquals(0, saved.size());
    }

    @Test
    void recoveryAlarmIncludesSelectionAndRemainingCounts() {
        List<AlarmInfo> saved = new ArrayList<>();
        DqlEventAlarmService service = service(saved, false);
        DqlRecoveryBatchDto batch = new DqlRecoveryBatchDto();
        batch.setBatchId("batch-1");
        batch.setTaskId("task-1");
        batch.setTaskName("Orders");
        batch.setOperatorName("operator");
        batch.setStatus("PARTIAL_FAILED");
        batch.setSelectedCount(3);
        batch.setSuccessCount(1);
        batch.setFailedCount(1);
        batch.setSkippedCount(1);

        service.notifyBatchPartialFailed(batch);

        assertEquals(1, saved.size());
        Map<String, Object> params = saved.get(0).getParam();
        assertEquals(3, params.get("selectedCount"));
        assertEquals(0L, params.get("remainingCount"));
        assertEquals("/exception-events?taskId=task-1", params.get("pageUrl"));
    }

    @Test
    void savesStormGuardAlarmFromEngineReport() {
        List<AlarmInfo> saved = new ArrayList<>();
        DqlEventAlarmService service = service(saved, false);
        DqlStormGuardReportVo report = new DqlStormGuardReportVo();
        report.setTaskId("task-1");
        report.setTaskName("Orders");
        report.setAgentId("agent-1");
        report.setGuardKey("guard-sha256");
        report.setWindowSeconds(60L);
        report.setGuardThreshold(20L);
        report.setSuppressedCountEstimate(3L);
        report.setRouteDecision("TASK_RETRY");
        report.setSafeReason("count exceeded");

        service.notifyStormGuard(report);

        assertEquals(1, saved.size());
        assertEquals(AlarmKeyEnum.TASK_DQL_STORM_GUARD, saved.get(0).getMetric());
        assertEquals("guard-sha256", saved.get(0).getParam().get("guardKey"));
        assertEquals("TASK_RETRY", saved.get(0).getParam().get("routeDecision"));
    }

    private DqlEventAlarmService service(List<AlarmInfo> saved, boolean fail) {
        DqlEventAlarmService service = new DqlEventAlarmService();
        service.setAlarmService((AlarmService) Proxy.newProxyInstance(
                AlarmService.class.getClassLoader(),
                new Class<?>[]{AlarmService.class},
                (proxy, method, args) -> {
                    if ("save".equals(method.getName())) {
                        if (fail) {
                            throw new IllegalStateException("alarm backend unavailable");
                        }
                        saved.add((AlarmInfo) args[0]);
                        return null;
                    }
                    if (method.getReturnType().isPrimitive()) {
                        return primitiveDefault(method.getReturnType());
                    }
                    return null;
                }));
        return service;
    }

    private Object primitiveDefault(Class<?> type) {
        if (type == boolean.class) return false;
        if (type == char.class) return '\0';
        if (type == byte.class) return (byte) 0;
        if (type == short.class) return (short) 0;
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (type == float.class) return 0F;
        if (type == double.class) return 0D;
        return null;
    }
}
