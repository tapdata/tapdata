package com.tapdata.tm.task.service;

import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.UnknownNode;
import com.tapdata.tm.commons.dag.process.JsProcessorNode;
import com.tapdata.tm.commons.dag.vo.TestRunDto;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.task.service.impl.TaskNodeServiceImpl;
import com.tapdata.tm.worker.entity.Worker;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
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

    private static Map<String, Object> jsNode(Object... extra) {
        Map<String, Object> node = new java.util.LinkedHashMap<>(Map.of("id", "js-1", "name", "js", "type", "js_processor", "script", "old script"));
        for (int i = 0; i < extra.length; i += 2) {
            node.put((String) extra[i], extra[i + 1]);
        }
        return node;
    }

    private static Map<String, Object> targetTable() {
        return Map.of("id", "table-2", "name", "orders_copy", "type", "table", "tableName", "orders_copy", "connectionId", "conn-2");
    }

    /** Sync task: {@code upstream -> js-1 -> table-2}. */
    private static TaskDto syncJsTask(Map<String, Object> upstream, Map<String, Object> js) {
        DAG dag = dag(List.of(Map.of("source", (String) upstream.get("id"), "target", "js-1"), Map.of("source", "js-1", "target", "table-2")),
                upstream, js, targetTable());
        TaskDto task = task(dag);
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        task.setTestTaskId(new ObjectId().toHexString());
        return task;
    }

    private static TestRunDto testRun(TaskDto task) {
        TestRunDto dto = new TestRunDto();
        dto.setTaskId(task.getId().toHexString());
        dto.setJsNodeId("js-1");
        dto.setScript("new script");
        dto.setRows(1);
        dto.setVersion(1L);
        return dto;
    }

    private static TaskNodeServiceImpl testRunService(TaskDto task, MessageQueueService messageQueue, WorkerService workers, AgentGroupService agentGroups) {
        return testRunService(task, messageQueue, workers, agentGroups, mock(MonitoringLogsService.class));
    }

    private static TaskNodeServiceImpl testRunService(TaskDto task, MessageQueueService messageQueue, WorkerService workers,
                                                      AgentGroupService agentGroups, MonitoringLogsService monitoringLogs) {
        TaskNodeServiceImpl service = new TaskNodeServiceImpl();
        TaskService taskService = mock(TaskService.class);
        when(taskService.findById(task.getId())).thenReturn(task);
        service.setTaskService(taskService);
        service.setMonitoringLogService(monitoringLogs);
        service.setMessageQueueService(messageQueue);
        service.setWorkerService(workers);
        service.setAgentGroupService(agentGroups);
        return service;
    }

    @Test
    @DisplayName("JS 试运行：上游含未注册类型节点时以 Task.TestRunUnknownNodeType 拒绝，旧版与 RPC 入口都不派发引擎、不清理旧试运行日志")
    void testRunRejectsUnknownUpstreamNode() {
        TaskDto task = syncJsTask(unknownNode(), jsNode());
        MessageQueueService messageQueue = mock(MessageQueueService.class);
        WorkerService workers = mock(WorkerService.class);
        AgentGroupService agentGroups = mock(AgentGroupService.class);
        MonitoringLogsService monitoringLogs = mock(MonitoringLogsService.class);
        TaskNodeServiceImpl service = testRunService(task, messageQueue, workers, agentGroups, monitoringLogs);

        BizException legacy = assertThrows(BizException.class, () -> service.testRunJsNode(testRun(task), user()));
        BizException rpc = assertThrows(BizException.class, () -> service.testRunJsNodeRPC(testRun(task), user(), 1));

        for (BizException error : List.of(legacy, rpc)) {
            assertEquals("Task.TestRunUnknownNodeType", error.getErrorCode());
            assertArrayEquals(new Object[]{UNKNOWN_TYPE}, error.getArgs());
        }
        // A rejected request must not clear the logs of the previous test run.
        verifyNoInteractions(messageQueue, workers, agentGroups, monitoringLogs);
    }

    @Test
    @DisplayName("JS 试运行：被试运行的 js_processor 因字段值无法绑定而保留为 UnknownNode 时，两个入口都拒绝，不派发引擎、不清理旧试运行日志")
    void testRunRejectsUnbindableJsNodeUnderTest() {
        TaskDto task = syncJsTask(tableNode(), jsNode("catalog", "aiProcessor"));
        assertTrue(task.getDag().getNode("js-1") instanceof UnknownNode);
        MessageQueueService messageQueue = mock(MessageQueueService.class);
        MonitoringLogsService monitoringLogs = mock(MonitoringLogsService.class);
        TaskNodeServiceImpl service = testRunService(task, messageQueue, mock(WorkerService.class), mock(AgentGroupService.class), monitoringLogs);

        BizException legacy = assertThrows(BizException.class, () -> service.testRunJsNode(testRun(task), user()));
        BizException rpc = assertThrows(BizException.class, () -> service.testRunJsNodeRPC(testRun(task), user(), 1));

        for (BizException error : List.of(legacy, rpc)) {
            assertEquals("Task.TestRunUnknownNodeType", error.getErrorCode());
            assertArrayEquals(new Object[]{"js_processor"}, error.getArgs());
        }
        verifyNoInteractions(messageQueue, monitoringLogs);
    }

    @Test
    @DisplayName("JS 试运行：不含占位节点时照常派发，并应用本次提交的脚本")
    void testRunStillAppliesSubmittedScriptForRegisteredNodes() {
        TaskDto task = syncJsTask(tableNode(), jsNode());
        MessageQueueService messageQueue = mock(MessageQueueService.class);
        WorkerService workers = mock(WorkerService.class);
        AgentGroupService agentGroups = mock(AgentGroupService.class);
        Worker worker = new Worker();
        worker.setProcessId("agent-1");
        when(workers.findAvailableAgentByAccessNode(any(), any())).thenReturn(List.of(worker));
        TaskNodeServiceImpl service = testRunService(task, messageQueue, workers, agentGroups);

        service.testRunJsNode(testRun(task), user());

        ArgumentCaptor<MessageQueueDto> sent = ArgumentCaptor.forClass(MessageQueueDto.class);
        verify(messageQueue).sendMessage(sent.capture());
        TaskDto testRunTask = (TaskDto) sent.getValue().getData();
        Node<?> js = testRunTask.getDag().getNode("js-1");
        assertTrue(js instanceof JsProcessorNode, String.valueOf(js));
        assertEquals("new script", ((JsProcessorNode) js).getScript());
    }

    @Test
    @DisplayName("复制任务：DAG 含 UnknownNode 时以 Task.CopyUnknownNodeType 拒绝，不改写节点 ID、不保存副本")
    void copyRejectsTaskWithUnknownNode() {
        TaskRepository repository = mock(TaskRepository.class);
        TaskServiceImpl service = spy(new TaskServiceImpl(repository));
        DAG dag = dag(List.of(Map.of("source", "table-1", "target", "unknown-1")), tableNode(), unknownNode());
        TaskDto task = task(dag);
        UserDetail user = user();
        doReturn(task).when(service).checkExistById(task.getId(), user);

        BizException error = assertThrows(BizException.class, () -> service.copy(task.getId(), user));

        assertEquals("Task.CopyUnknownNodeType", error.getErrorCode());
        assertArrayEquals(new Object[]{UNKNOWN_TYPE}, error.getArgs());
        assertNotNull(task.getDag().getNode("unknown-1"), "node ids must not be rewritten");
        verifyNoInteractions(repository);
    }

    @Test
    @DisplayName("导入后的 JS 试运行检查：js_processor 保留为 UnknownNode 时跳过，不抛 ClassCastException")
    void importTestRunCheckSkipsUnknownJsNode() {
        TaskServiceImpl service = new TaskServiceImpl(mock(TaskRepository.class));
        TaskNodeService taskNodeService = mock(TaskNodeService.class);
        ReflectionTestUtils.setField(service, "taskNodeService", taskNodeService);
        TaskDto task = syncJsTask(tableNode(), jsNode("catalog", "aiProcessor"));

        assertDoesNotThrow(() -> service.checkJsProcessorTestRun(user(), List.of(task)));

        verifyNoInteractions(taskNodeService);
    }
}
