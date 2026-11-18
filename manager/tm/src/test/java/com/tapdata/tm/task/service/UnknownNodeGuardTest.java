package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.monitor.dto.TaskLogDto;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.schedule.TaskRestartSchedule;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.entity.TaskDagCheckLog;
import com.tapdata.tm.task.service.impl.TaskDagCheckLogServiceImpl;
import com.tapdata.tm.task.service.impl.TaskScheduleServiceImpl;
import com.tapdata.tm.task.service.impl.dagcheckstrategy.DataInspectStrategyImpl;
import com.tapdata.tm.task.utils.CacheUtils;
import com.tapdata.tm.task.vo.TaskDagCheckLogVo;
import com.tapdata.tm.worker.service.WorkerService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Task check and scheduling paths that previously assumed every node type is registered in this version. */
class UnknownNodeGuardTest {

    private static final String UNKNOWN_TYPE = "dev_only_processor";

    @SafeVarargs
    private static DAG dag(List<Map<String, String>> edges, Map<String, Object>... nodes) {
        String json = JsonUtil.toJsonUseJackson(Map.of("nodes", List.of(nodes), "edges", edges));
        return DAG.build(JsonUtil.parseJsonUseJackson(json, Dag.class));
    }

    private static Map<String, Object> unknownNode() {
        return Map.of("id", "unknown-1", "name", "dev only node", "type", UNKNOWN_TYPE);
    }

    private static Map<String, Object> tableNode() {
        return Map.of("id", "table-1", "name", "orders", "type", "table", "tableName", "orders", "connectionId", "conn-1");
    }

    private static TaskDto task(DAG dag) {
        TaskDto task = new TaskDto();
        task.setId(new ObjectId());
        task.setName("task left by a dev build");
        task.setSyncType(TaskDto.SYNC_TYPE_MIGRATE);
        task.setDag(dag);
        return task;
    }

    private static UserDetail user() {
        UserDetail user = mock(UserDetail.class);
        when(user.getUserId()).thenReturn("user-1");
        return user;
    }

    @Test
    @DisplayName("数据校验检查把 UnknownNode 计入不支持校验的节点，不再报 check error")
    void dataInspectReportsUnknownNodeAsUnsupported() {
        TaskDto task = task(dag(List.of(Map.of("source", "table-1", "target", "unknown-1")), tableNode(), unknownNode()));
        task.setIsAutoInspect(true);
        task.setCanOpenInspect(true);

        List<TaskDagCheckLog> logs = new DataInspectStrategyImpl().getLogs(task, user(), Locale.US);

        assertEquals(1, logs.size());
        TaskDagCheckLog log = logs.get(0);
        assertEquals(Level.ERROR, log.getGrade());
        assertTrue(log.getLog().contains("(" + UNKNOWN_TYPE + ")"), log.getLog());
        assertFalse(log.getLog().contains("check error"), log.getLog());
    }

    @Test
    @DisplayName("迁移任务源节点为 UnknownNode（非 DatabaseNode）时，DAG 检查模型统计不抛 NoSuchElementException")
    void dagCheckLogsToleratesUnknownSourceNode() {
        DAG dag = dag(List.of(Map.of("source", "unknown-1", "target", "table-1")), unknownNode(), tableNode());
        TaskDto task = task(dag);
        task.setTransformed(true);

        TaskService taskService = mock(TaskService.class);
        when(taskService.findById(any())).thenReturn(task);
        MetadataInstancesDto schema = new MetadataInstancesDto();
        schema.setNodeId("table-1");
        schema.setName("orders");
        MetadataInstancesService metadataInstancesService = mock(MetadataInstancesService.class);
        when(metadataInstancesService.findByTaskId(any(), any())).thenReturn(List.of(schema));
        TaskDagCheckLogServiceImpl service = new TaskDagCheckLogServiceImpl();
        ReflectionTestUtils.setField(service, "taskService", taskService);
        ReflectionTestUtils.setField(service, "metadataInstancesService", metadataInstancesService);

        TaskLogDto dto = new TaskLogDto();
        dto.setTaskId(task.getId().toHexString());
        try (MockedStatic<CacheUtils> cache = mockStatic(CacheUtils.class)) {
            cache.when(() -> CacheUtils.isExist(any())).thenReturn(true);
            cache.when(() -> CacheUtils.get(any())).thenReturn(new ArrayList<TaskDagCheckLog>());

            TaskDagCheckLogVo vo = assertDoesNotThrow(() -> service.getLogs(dto, user(), Locale.US));

            assertNotNull(vo);
            assertTrue(vo.getNodes().containsKey("unknown-1"));
        }
    }

    @Test
    @DisplayName("云版引擎重启恢复：含未知节点的任务被拒绝，不直接发送启动消息")
    void cloudRestartDoesNotSendStartForUnknownNodes() {
        TaskRestartSchedule schedule = new TaskRestartSchedule();
        TaskScheduleService taskScheduleService = mock(TaskScheduleService.class);
        WorkerService workerService = mock(WorkerService.class);
        schedule.setTaskScheduleService(taskScheduleService);
        schedule.setWorkerService(workerService);
        TaskDto task = task(dag(List.of(), unknownNode()));
        task.setAgentId("agent-1");
        UserDetail user = user();
        when(taskScheduleService.rejectUnknownNodes(task, user)).thenReturn(true);

        Boolean skipped = ReflectionTestUtils.invokeMethod(schedule, "restartInCloud", true, task, user);

        assertEquals(Boolean.TRUE, skipped);
        verify(taskScheduleService).rejectUnknownNodes(task, user);
        verify(taskScheduleService, never()).sendStartMsg(any(), any(), any());
        verifyNoInteractions(workerService);
    }

    @Test
    @DisplayName("云版引擎重启恢复：不含未知节点的任务在引擎在线时仍直接发送启动消息")
    void cloudRestartStillSendsStartForRegisteredNodes() {
        TaskRestartSchedule schedule = new TaskRestartSchedule();
        TaskScheduleService taskScheduleService = mock(TaskScheduleService.class);
        WorkerService workerService = mock(WorkerService.class);
        schedule.setTaskScheduleService(taskScheduleService);
        schedule.setWorkerService(workerService);
        TaskDto task = task(dag(List.of(), tableNode()));
        task.setAgentId("agent-1");
        UserDetail user = user();
        when(workerService.checkUsedAgent("agent-1", user)).thenReturn("online");

        Boolean skipped = ReflectionTestUtils.invokeMethod(schedule, "restartInCloud", true, task, user);

        assertEquals(Boolean.TRUE, skipped);
        verify(taskScheduleService).sendStartMsg(task.getId().toHexString(), "agent-1", user);
    }

    private static String rejectAndCaptureLog(StateMachineResult transition) {
        TaskScheduleServiceImpl scheduler = new TaskScheduleServiceImpl();
        StateMachineService stateMachine = mock(StateMachineService.class);
        MonitoringLogsService monitoringLogs = mock(MonitoringLogsService.class);
        scheduler.setStateMachineService(stateMachine);
        scheduler.setMonitoringLogsService(monitoringLogs);
        TaskDto task = task(dag(List.of(), unknownNode()));
        task.setStatus(TaskDto.STATUS_SCHEDULING);
        UserDetail user = user();
        when(stateMachine.executeAboutTask(task, DataFlowEvent.ERROR, user)).thenReturn(transition);

        assertTrue(scheduler.rejectUnknownNodes(task, user));

        verify(stateMachine).executeAboutTask(task, DataFlowEvent.ERROR, user);
        ArgumentCaptor<String> message = ArgumentCaptor.forClass(String.class);
        verify(monitoringLogs).startTaskErrorLog(eq(task), eq(user), message.capture(), eq(Level.ERROR));
        assertTrue(message.getValue().contains(UNKNOWN_TYPE), message.getValue());
        return message.getValue();
    }

    @Test
    @DisplayName("调度拦截：任务已转 ERROR 时，监控日志说明已转错误状态")
    void rejectReportsErrorStatusWhenTransitionSucceeds() {
        String message = rejectAndCaptureLog(StateMachineResult.ok(1));

        assertTrue(message.contains("set to error"), message);
    }

    @Test
    @DisplayName("调度拦截：ERROR 状态迁移失败（如并发改变状态）时，监控日志不声称已转错误状态")
    void rejectDoesNotClaimErrorStatusWhenTransitionFails() {
        String message = rejectAndCaptureLog(StateMachineResult.fail("status changed concurrently"));

        assertFalse(message.contains("set to error"), message);
        assertTrue(message.contains("not scheduled"), message);
    }
}
