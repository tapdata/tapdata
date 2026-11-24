package com.tapdata.tm.task.service;

import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.alarm.Level;
import com.tapdata.tm.commons.dag.vo.TestRunDto;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.schedule.TaskRestartSchedule;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.task.service.impl.TaskNodeServiceImpl;
import com.tapdata.tm.task.service.impl.TaskScheduleServiceImpl;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verifyNoInteractions;

/** Real task start with a DAG parsed by the production node deserializer; storage is the external boundary. */
@Timeout(10)
class TaskUnknownNodeStartIT {

    @SafeVarargs
    private static DAG dag(Map<String, Object>... nodes) {
        String json = JsonUtil.toJsonUseJackson(Map.of("nodes", List.of(nodes), "edges", List.of()));
        return DAG.build(JsonUtil.parseJsonUseJackson(json, Dag.class));
    }

    private static TaskDto task(DAG dag) {
        TaskDto task = new TaskDto();
        task.setName("task left by a dev build");
        task.setSyncType(TaskDto.SYNC_TYPE_MIGRATE);
        task.setType("initial_sync");
        task.setStatus(TaskDto.STATUS_EDIT);
        task.setDag(dag);
        return task;
    }

    @Test
    @DisplayName("L2：启动含当前版本未注册节点类型的任务，在任何持久化之前以 Task.UnknownNodeType 拒绝")
    void shouldRejectUnknownNodeBeforePersistingStart() {
        TaskRepository tasks = mock(TaskRepository.class);
        TaskServiceImpl service = new TaskServiceImpl(tasks);
        TaskDto task = task(dag(
                Map.of("id", "table-1", "type", "table", "tableName", "orders"),
                Map.of("id", "unknown-2", "type", "dev_z_processor"),
                Map.of("id", "unknown-1", "type", "dev_a_processor", "devScript", "return record;")));

        BizException error = assertThrows(BizException.class, () -> service.start(task, mock(UserDetail.class), "11"));

        assertEquals("Task.UnknownNodeType", error.getErrorCode());
        assertArrayEquals(new Object[]{"dev_a_processor, dev_z_processor"}, error.getArgs());
        assertNull(task.getLastStartDate());
        verifyNoInteractions(tasks);
    }

    @Test
    @DisplayName("L2：只含已注册节点类型的任务不触发未知节点校验")
    void shouldAcceptTaskWithRegisteredNodesOnly() {
        TaskServiceImpl service = new TaskServiceImpl(mock(TaskRepository.class));
        TaskDto task = task(dag(Map.of("id", "table-1", "type", "table", "tableName", "orders")));

        assertDoesNotThrow(() -> service.validateUnknownNodes(task));
    }

    @Test
    @DisplayName("L2：调度失败自动重试经真实 TaskScheduleServiceImpl 调度含未知节点的任务时，写 ERROR 日志并转错误状态，不分配引擎、不发送启动消息")
    void shouldRejectUnknownNodeOnAutomaticRescheduling() {
        TaskService tasks = mock(TaskService.class);
        UserService users = mock(UserService.class);
        WorkerService workers = mock(WorkerService.class);
        MonitoringLogsService monitoringLogs = mock(MonitoringLogsService.class);
        StateMachineService stateMachine = mock(StateMachineService.class);
        MessageQueueService messageQueue = mock(MessageQueueService.class);

        // Separate mock for the scheduler's own task reads/writes, so "no agent calculation, no agentId write" is observable.
        TaskService schedulerTasks = mock(TaskService.class);
        TaskScheduleServiceImpl scheduler = new TaskScheduleServiceImpl();
        scheduler.setTaskService(schedulerTasks);
        scheduler.setUserService(users);
        scheduler.setWorkerService(workers);
        scheduler.setMonitoringLogsService(monitoringLogs);
        scheduler.setStateMachineService(stateMachine);
        scheduler.setMessageQueueService(messageQueue);
        TaskRestartSchedule restartSchedule = new TaskRestartSchedule();
        restartSchedule.setTaskService(tasks);
        restartSchedule.setUserService(users);
        restartSchedule.setWorkerService(workers);
        restartSchedule.setMonitoringLogsService(monitoringLogs);
        restartSchedule.setStateMachineService(stateMachine);
        restartSchedule.setSettingsService(mock(SettingsService.class));
        restartSchedule.setTransformSchema(mock(TransformSchemaService.class));
        restartSchedule.setTaskScheduleService(scheduler);

        // A task left by a dev build that was SCHEDULE_FAILED when the new version was deployed.
        TaskDto task = task(dag(
                Map.of("id", "table-1", "type", "table", "tableName", "orders"),
                Map.of("id", "unknown-2", "type", "dev_z_processor"),
                Map.of("id", "unknown-1", "type", "dev_a_processor")));
        task.setId(new ObjectId());
        task.setUserId("user-1");
        task.setStatus(TaskDto.STATUS_SCHEDULE_FAILED);
        UserDetail user = mock(UserDetail.class);
        when(user.getUserId()).thenReturn("user-1");
        Worker worker = new Worker();
        worker.setUserId("user-1");
        worker.setProcessId("agent-1");
        when(tasks.findAll(any(Query.class))).thenReturn(List.of(task));
        when(users.getUserByIdList(anyList())).thenReturn(List.of(user));
        when(workers.findAvailableAgentBySystem(anyList())).thenReturn(List.of(worker));
        when(stateMachine.executeAboutTask(any(TaskDto.class), any(DataFlowEvent.class), any(UserDetail.class)))
                .thenReturn(StateMachineResult.ok());

        restartSchedule.retrySchedulingFailedTask();

        ArgumentCaptor<String> message = ArgumentCaptor.forClass(String.class);
        verify(monitoringLogs).startTaskErrorLog(eq(task), eq(user), message.capture(), eq(Level.ERROR));
        assertTrue(message.getValue().contains("dev_a_processor, dev_z_processor"), message.getValue());
        verify(stateMachine).executeAboutTask(task, DataFlowEvent.OVERTIME, user);
        verify(stateMachine).executeAboutTask(task, DataFlowEvent.ERROR, user);
        verify(stateMachine, never()).executeAboutTask(any(TaskDto.class), eq(DataFlowEvent.SCHEDULE_SUCCESS), any(UserDetail.class));
        verifyNoInteractions(schedulerTasks, messageQueue);
        assertNull(task.getAgentId());
    }

    /** Sync task {@code table-1 -> unknown-1 -> js-1 -> table-2}, parsed by the production node deserializer. */
    private static TaskDto syncTaskWithUnknownUpstream() {
        String json = JsonUtil.toJsonUseJackson(Map.of(
                "nodes", List.of(
                        Map.of("id", "table-1", "type", "table", "tableName", "orders", "connectionId", "conn-1"),
                        Map.of("id", "unknown-1", "type", "dev_a_processor", "devScript", "return record;"),
                        Map.of("id", "js-1", "type", "js_processor", "script", "old script"),
                        Map.of("id", "table-2", "type", "table", "tableName", "orders_copy", "connectionId", "conn-2")),
                "edges", List.of(
                        Map.of("source", "table-1", "target", "unknown-1"),
                        Map.of("source", "unknown-1", "target", "js-1"),
                        Map.of("source", "js-1", "target", "table-2"))));
        TaskDto task = task(DAG.build(JsonUtil.parseJsonUseJackson(json, Dag.class)));
        task.setId(new ObjectId());
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        task.setTestTaskId(new ObjectId().toHexString());
        return task;
    }

    @Test
    @DisplayName("L2：JS 试运行子图含未注册类型节点时，旧版与 RPC 入口都以 Task.TestRunUnknownNodeType 拒绝，不清理旧试运行日志、不查找引擎、不发送消息")
    void shouldRejectTestRunWhoseSubgraphContainsUnknownNode() {
        TaskDto task = syncTaskWithUnknownUpstream();
        TaskService tasks = mock(TaskService.class);
        when(tasks.findById(task.getId())).thenReturn(task);
        MessageQueueService messageQueue = mock(MessageQueueService.class);
        WorkerService workers = mock(WorkerService.class);
        AgentGroupService agentGroups = mock(AgentGroupService.class);
        MonitoringLogsService monitoringLogs = mock(MonitoringLogsService.class);
        TaskNodeServiceImpl service = new TaskNodeServiceImpl();
        service.setTaskService(tasks);
        service.setMonitoringLogService(monitoringLogs);
        service.setMessageQueueService(messageQueue);
        service.setWorkerService(workers);
        service.setAgentGroupService(agentGroups);
        TestRunDto dto = new TestRunDto();
        dto.setTaskId(task.getId().toHexString());
        dto.setJsNodeId("js-1");
        dto.setScript("new script");
        dto.setRows(1);
        dto.setVersion(1L);

        BizException legacy = assertThrows(BizException.class, () -> service.testRunJsNode(dto, mock(UserDetail.class)));
        BizException rpc = assertThrows(BizException.class, () -> service.testRunJsNodeRPC(dto, mock(UserDetail.class), 1));

        for (BizException error : List.of(legacy, rpc)) {
            assertEquals("Task.TestRunUnknownNodeType", error.getErrorCode());
            assertArrayEquals(new Object[]{"dev_a_processor"}, error.getArgs());
        }
        // Rejected before anything else: the logs of the previous test run are not cleared either.
        verifyNoInteractions(messageQueue, workers, agentGroups, monitoringLogs);
    }

    @Test
    @DisplayName("L2：复制含未注册类型节点的任务以 Task.CopyUnknownNodeType 拒绝，节点 ID 不被改写、不写库")
    void shouldRejectCopyOfTaskWithUnknownNode() {
        TaskRepository tasks = mock(TaskRepository.class);
        TaskServiceImpl service = spy(new TaskServiceImpl(tasks));
        TaskDto task = syncTaskWithUnknownUpstream();
        UserDetail user = mock(UserDetail.class);
        doReturn(task).when(service).checkExistById(task.getId(), user);

        BizException error = assertThrows(BizException.class, () -> service.copy(task.getId(), user));

        assertEquals("Task.CopyUnknownNodeType", error.getErrorCode());
        assertArrayEquals(new Object[]{"dev_a_processor"}, error.getArgs());
        assertEquals("unknown-1", task.getDag().getNode("unknown-1").getId());
        verifyNoInteractions(tasks);
    }
}
