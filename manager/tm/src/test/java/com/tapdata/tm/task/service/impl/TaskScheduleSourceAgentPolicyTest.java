package com.tapdata.tm.task.service.impl;

import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.base.dto.SchedulableDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.never;

class TaskScheduleSourceAgentPolicyTest {

    @Test
    void sharedMiningTaskUsesSourceAgentPolicyWithoutChangingTaskPolicy() {
        TaskScheduleServiceImpl service = new TaskScheduleServiceImpl();
        TaskService taskService = mock(TaskService.class);
        WorkerService workerService = mock(WorkerService.class);
        AgentGroupService agentGroupService = mock(AgentGroupService.class);
        UserService userService = mock(UserService.class);
        DataSourceService dataSourceService = mock(DataSourceService.class);
        SettingsService settingsService = mock(SettingsService.class);

        ReflectionTestUtils.setField(service, "taskService", taskService);
        ReflectionTestUtils.setField(service, "workerService", workerService);
        ReflectionTestUtils.setField(service, "agentGroupService", agentGroupService);
        ReflectionTestUtils.setField(service, "userService", userService);
        ReflectionTestUtils.setField(service, "dataSourceService", dataSourceService);
        ReflectionTestUtils.setField(service, "settingsService", settingsService);

        String userId = new ObjectId().toHexString();
        UserDetail user = mock(UserDetail.class);
        when(user.getUserId()).thenReturn(userId);
        when(settingsService.isCloud()).thenReturn(false);
        when(userService.loadUserById(any(ObjectId.class))).thenReturn(user);

        ObjectId taskId = new ObjectId();
        String connectionId = new ObjectId().toHexString();
        TaskDto task = new TaskDto();
        task.setId(taskId);
        task.setUserId(userId);
        task.setName("shared");
        task.setSyncType(TaskDto.SYNC_TYPE_LOG_COLLECTOR);
        task.setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
        task.setAgentId("old-agent");

        LogCollectorNode logCollectorNode = new LogCollectorNode();
        logCollectorNode.setConnectionIds(List.of(connectionId));
        DAG dag = mock(DAG.class);
        when(dag.getSources()).thenReturn(List.of((Node) logCollectorNode));
        task.setDag(dag);

        TaskDto taskOwner = new TaskDto();
        taskOwner.setUserId(userId);
        when(taskService.findByTaskId(eq(taskId), anyString())).thenReturn(taskOwner);
        when(dataSourceService.findInfoByConnectionIdList(eq(List.of(connectionId)), eq(user), any(String[].class)))
                .thenReturn(List.of(sourceConnection(connectionId, "agent-a")));
        when(agentGroupService.getProcessNodeListWithGroup(any(TaskDto.class), eq(user)))
                .thenReturn(List.of("agent-a"));
        Worker sourceAgent = new Worker();
        sourceAgent.setProcessId("agent-a");
        when(workerService.findAvailableAgentByAccessNode(eq(user), anyList())).thenReturn(List.of(sourceAgent));
        when(workerService.findByProcessId(anyString(), eq(user), any(String[].class))).thenReturn(new WorkerDto());
        when(workerService.getLimitTaskNum(any(WorkerDto.class), eq(user))).thenReturn(10);
        when(taskService.runningTaskNum(anyString(), eq(user))).thenReturn(0);
        when(taskService.subCronOrPlanNum(any(TaskDto.class), eq(0))).thenReturn(0);

        CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
        calculationEngineVo.setProcessId("agent-a");
        calculationEngineVo.setRunningNum(0);
        calculationEngineVo.setTaskLimit(10);
        when(workerService.scheduleTaskToEngineWithStrictAgent(any(SchedulableDto.class), eq(user), eq("task"), eq("shared")))
                .thenReturn(calculationEngineVo);

        service.cloudTaskLimitNum(task, user, true);

        org.mockito.ArgumentCaptor<SchedulableDto> scheduledTask = org.mockito.ArgumentCaptor.forClass(SchedulableDto.class);
        verify(workerService, times(1)).scheduleTaskToEngineWithStrictAgent(scheduledTask.capture(), eq(user), eq("task"), eq("shared"));
        TaskDto scheduledTaskDto = (TaskDto) scheduledTask.getValue();
        assertEquals(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), scheduledTaskDto.getAccessNodeType());
        assertEquals("agent-a", scheduledTaskDto.getAccessNodeProcessId());
        assertEquals(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name(), task.getAccessNodeType());
        assertNull(task.getAccessNodeProcessId());
        assertEquals("agent-a", task.getAgentId());
    }

    @Test
    void unavailableSourceAgentKeepsTaskPendingInsteadOfThrowing() {
        TaskScheduleServiceImpl service = new TaskScheduleServiceImpl();
        TaskService taskService = mock(TaskService.class);
        WorkerService workerService = mock(WorkerService.class);
        AgentGroupService agentGroupService = mock(AgentGroupService.class);
        UserService userService = mock(UserService.class);
        DataSourceService dataSourceService = mock(DataSourceService.class);
        SettingsService settingsService = mock(SettingsService.class);

        ReflectionTestUtils.setField(service, "taskService", taskService);
        ReflectionTestUtils.setField(service, "workerService", workerService);
        ReflectionTestUtils.setField(service, "agentGroupService", agentGroupService);
        ReflectionTestUtils.setField(service, "userService", userService);
        ReflectionTestUtils.setField(service, "dataSourceService", dataSourceService);
        ReflectionTestUtils.setField(service, "settingsService", settingsService);

        String userId = new ObjectId().toHexString();
        UserDetail user = mock(UserDetail.class);
        when(user.getUserId()).thenReturn(userId);
        when(settingsService.isCloud()).thenReturn(false);
        when(userService.loadUserById(any(ObjectId.class))).thenReturn(user);

        ObjectId taskId = new ObjectId();
        String connectionId = new ObjectId().toHexString();
        TaskDto task = new TaskDto();
        task.setId(taskId);
        task.setUserId(userId);
        task.setName("shared");
        task.setSyncType(TaskDto.SYNC_TYPE_LOG_COLLECTOR);
        task.setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
        task.setAgentId("stale-agent");

        LogCollectorNode source = new LogCollectorNode();
        source.setConnectionIds(List.of(connectionId));
        DAG dag = mock(DAG.class);
        when(dag.getSources()).thenReturn(List.of((Node) source));
        task.setDag(dag);

        TaskDto taskOwner = new TaskDto();
        taskOwner.setUserId(userId);
        when(taskService.findByTaskId(eq(taskId), anyString())).thenReturn(taskOwner);
        when(dataSourceService.findInfoByConnectionIdList(eq(List.of(connectionId)), eq(user), any(String[].class)))
                .thenReturn(List.of(sourceConnection(connectionId, "source-agent", "source")));
        when(agentGroupService.getProcessNodeListWithGroup(any(TaskDto.class), eq(user)))
                .thenReturn(List.of("source-agent"));
        when(workerService.findAvailableAgentByAccessNode(eq(user), anyList())).thenReturn(List.of());
        when(workerService.findByProcessId(anyString(), eq(user), any(String[].class))).thenReturn(new WorkerDto());
        when(workerService.getLimitTaskNum(any(WorkerDto.class), eq(user))).thenReturn(10);
        when(taskService.runningTaskNum(anyString(), eq(user))).thenReturn(0);
        when(taskService.subCronOrPlanNum(any(TaskDto.class), eq(0))).thenReturn(0);
        when(workerService.scheduleTaskToEngineWithStrictAgent(any(SchedulableDto.class), eq(user), eq("task"), eq("shared")))
                .thenThrow(new BizException("Task.AgentNotFound"));

        CalculationEngineVo result = service.cloudTaskLimitNum(task, user, true);

        assertEquals(0, result.getAvailable());
        assertNull(task.getAgentId());
        verify(workerService).scheduleTaskToEngineWithStrictAgent(any(SchedulableDto.class), eq(user), eq("task"), eq("shared"));
    }

    @Test
    void missingSourceConnectionFallsBackToTaskPolicy() {
        TaskScheduleServiceImpl service = new TaskScheduleServiceImpl();
        TaskService taskService = mock(TaskService.class);
        WorkerService workerService = mock(WorkerService.class);
        AgentGroupService agentGroupService = mock(AgentGroupService.class);
        UserService userService = mock(UserService.class);
        DataSourceService dataSourceService = mock(DataSourceService.class);
        SettingsService settingsService = mock(SettingsService.class);

        ReflectionTestUtils.setField(service, "taskService", taskService);
        ReflectionTestUtils.setField(service, "workerService", workerService);
        ReflectionTestUtils.setField(service, "agentGroupService", agentGroupService);
        ReflectionTestUtils.setField(service, "userService", userService);
        ReflectionTestUtils.setField(service, "dataSourceService", dataSourceService);
        ReflectionTestUtils.setField(service, "settingsService", settingsService);

        String userId = new ObjectId().toHexString();
        UserDetail user = mock(UserDetail.class);
        when(user.getUserId()).thenReturn(userId);
        when(settingsService.isCloud()).thenReturn(false);
        when(userService.loadUserById(any(ObjectId.class))).thenReturn(user);

        ObjectId taskId = new ObjectId();
        String connectionId = new ObjectId().toHexString();
        TaskDto task = new TaskDto();
        task.setId(taskId);
        task.setUserId(userId);
        task.setName("shared");
        task.setSyncType(TaskDto.SYNC_TYPE_LOG_COLLECTOR);
        task.setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());

        LogCollectorNode source = new LogCollectorNode();
        source.setConnectionIds(List.of(connectionId));
        DAG dag = mock(DAG.class);
        when(dag.getSources()).thenReturn(List.of((Node) source));
        task.setDag(dag);

        TaskDto taskOwner = new TaskDto();
        taskOwner.setUserId(userId);
        when(taskService.findByTaskId(eq(taskId), anyString())).thenReturn(taskOwner);
        when(dataSourceService.findInfoByConnectionIdList(eq(List.of(connectionId)), eq(user), any(String[].class)))
                .thenReturn(List.of());
        when(agentGroupService.getProcessNodeListWithGroup(any(TaskDto.class), eq(user))).thenReturn(List.of());
        when(workerService.findAvailableAgentByAccessNode(eq(user), anyList())).thenReturn(List.of());
        when(taskService.runningTaskNum(any(UserDetail.class))).thenReturn(0);
        CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
        calculationEngineVo.setProcessId("task-agent");
        calculationEngineVo.setRunningNum(0);
        calculationEngineVo.setTaskLimit(10);
        when(workerService.scheduleTaskToEngine(any(SchedulableDto.class), eq(user), eq("task"), eq("shared")))
                .thenReturn(calculationEngineVo);

        CalculationEngineVo result = service.cloudTaskLimitNum(task, user, true);

        assertEquals("task-agent", result.getProcessId());
        verify(workerService).scheduleTaskToEngine(any(SchedulableDto.class), eq(user), eq("task"), eq("shared"));
        verify(workerService, never()).scheduleTaskToEngineWithStrictAgent(any(SchedulableDto.class), eq(user), eq("task"), eq("shared"));
    }

    @Test
    void conflictingSourceAgentPoliciesAreReportedWithConnectionNames() {
        TaskScheduleServiceImpl service = new TaskScheduleServiceImpl();
        TaskService taskService = mock(TaskService.class);
        WorkerService workerService = mock(WorkerService.class);
        AgentGroupService agentGroupService = mock(AgentGroupService.class);
        UserService userService = mock(UserService.class);
        DataSourceService dataSourceService = mock(DataSourceService.class);
        SettingsService settingsService = mock(SettingsService.class);

        ReflectionTestUtils.setField(service, "taskService", taskService);
        ReflectionTestUtils.setField(service, "workerService", workerService);
        ReflectionTestUtils.setField(service, "agentGroupService", agentGroupService);
        ReflectionTestUtils.setField(service, "userService", userService);
        ReflectionTestUtils.setField(service, "dataSourceService", dataSourceService);
        ReflectionTestUtils.setField(service, "settingsService", settingsService);

        String userId = new ObjectId().toHexString();
        UserDetail user = mock(UserDetail.class);
        when(user.getUserId()).thenReturn(userId);
        when(settingsService.isCloud()).thenReturn(false);
        when(userService.loadUserById(any(ObjectId.class))).thenReturn(user);

        ObjectId taskId = new ObjectId();
        String firstConnectionId = new ObjectId().toHexString();
        String secondConnectionId = new ObjectId().toHexString();
        TaskDto task = new TaskDto();
        task.setId(taskId);
        task.setUserId(userId);
        task.setName("shared");
        task.setSyncType(TaskDto.SYNC_TYPE_LOG_COLLECTOR);
        task.setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());

        LogCollectorNode firstSource = new LogCollectorNode();
        firstSource.setConnectionIds(List.of(firstConnectionId));
        LogCollectorNode secondSource = new LogCollectorNode();
        secondSource.setConnectionIds(List.of(secondConnectionId));
        DAG dag = mock(DAG.class);
        when(dag.getSources()).thenReturn(List.of((Node) firstSource, secondSource));
        task.setDag(dag);

        TaskDto taskOwner = new TaskDto();
        taskOwner.setUserId(userId);
        when(taskService.findByTaskId(eq(taskId), anyString())).thenReturn(taskOwner);
        when(dataSourceService.findInfoByConnectionIdList(eq(List.of(firstConnectionId, secondConnectionId)), eq(user), any(String[].class)))
                .thenReturn(List.of(sourceConnection(firstConnectionId, "agent-a", "orders"),
                        sourceConnection(secondConnectionId, "agent-b", "users")));

        BizException exception = assertThrows(BizException.class, () -> service.cloudTaskLimitNum(task, user, true));

        assertEquals("Task.SourceAgentConflict", exception.getErrorCode());
        assertTrue(exception.getMessage().contains("orders"));
        assertTrue(exception.getMessage().contains("users"));
        verify(workerService, never()).scheduleTaskToEngine(any(SchedulableDto.class), any(UserDetail.class), anyString(), anyString());
        verify(workerService, never()).scheduleTaskToEngineWithStrictAgent(any(SchedulableDto.class), any(UserDetail.class), anyString(), anyString());
    }

    @Test
    void normalTaskDoesNotReadSourceAgentPolicy() {
        TaskScheduleServiceImpl service = new TaskScheduleServiceImpl();
        TaskService taskService = mock(TaskService.class);
        WorkerService workerService = mock(WorkerService.class);
        AgentGroupService agentGroupService = mock(AgentGroupService.class);
        UserService userService = mock(UserService.class);
        DataSourceService dataSourceService = mock(DataSourceService.class);
        SettingsService settingsService = mock(SettingsService.class);

        ReflectionTestUtils.setField(service, "taskService", taskService);
        ReflectionTestUtils.setField(service, "workerService", workerService);
        ReflectionTestUtils.setField(service, "agentGroupService", agentGroupService);
        ReflectionTestUtils.setField(service, "userService", userService);
        ReflectionTestUtils.setField(service, "dataSourceService", dataSourceService);
        ReflectionTestUtils.setField(service, "settingsService", settingsService);

        String userId = new ObjectId().toHexString();
        UserDetail user = mock(UserDetail.class);
        when(user.getUserId()).thenReturn(userId);
        when(settingsService.isCloud()).thenReturn(false);
        when(userService.loadUserById(any(ObjectId.class))).thenReturn(user);

        TaskDto task = new TaskDto();
        task.setId(new ObjectId());
        task.setUserId(userId);
        task.setName("normal");
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        task.setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());

        TaskDto taskOwner = new TaskDto();
        taskOwner.setUserId(userId);
        when(taskService.findByTaskId(any(ObjectId.class), anyString())).thenReturn(taskOwner);
        when(agentGroupService.getProcessNodeListWithGroup(any(TaskDto.class), eq(user))).thenReturn(List.of());
        when(workerService.findAvailableAgentByAccessNode(eq(user), anyList())).thenReturn(List.of());
        when(taskService.runningTaskNum(any(UserDetail.class))).thenReturn(0);
        when(workerService.scheduleTaskToEngine(any(SchedulableDto.class), eq(user), eq("task"), eq("normal")))
                .thenReturn(new CalculationEngineVo());

        service.cloudTaskLimitNum(task, user, true);

        verify(dataSourceService, never()).findInfoByConnectionIdList(anyList(), eq(user), any(String[].class));
    }

    @Test
    void heartbeatTaskUsesTargetSourceAgentPolicy() {
        TaskScheduleServiceImpl service = new TaskScheduleServiceImpl();
        TaskService taskService = mock(TaskService.class);
        WorkerService workerService = mock(WorkerService.class);
        AgentGroupService agentGroupService = mock(AgentGroupService.class);
        UserService userService = mock(UserService.class);
        DataSourceService dataSourceService = mock(DataSourceService.class);
        SettingsService settingsService = mock(SettingsService.class);

        ReflectionTestUtils.setField(service, "taskService", taskService);
        ReflectionTestUtils.setField(service, "workerService", workerService);
        ReflectionTestUtils.setField(service, "agentGroupService", agentGroupService);
        ReflectionTestUtils.setField(service, "userService", userService);
        ReflectionTestUtils.setField(service, "dataSourceService", dataSourceService);
        ReflectionTestUtils.setField(service, "settingsService", settingsService);

        String userId = new ObjectId().toHexString();
        UserDetail user = mock(UserDetail.class);
        when(user.getUserId()).thenReturn(userId);
        when(settingsService.isCloud()).thenReturn(false);
        when(userService.loadUserById(any(ObjectId.class))).thenReturn(user);

        ObjectId taskId = new ObjectId();
        String connectionId = new ObjectId().toHexString();
        TaskDto task = new TaskDto();
        task.setId(taskId);
        task.setUserId(userId);
        task.setName("heartbeat");
        task.setSyncType(TaskDto.SYNC_TYPE_CONN_HEARTBEAT);
        task.setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
        task.setAgentId("old-agent");

        TableNode sourceNode = new TableNode();
        sourceNode.setConnectionId(connectionId);
        DAG dag = mock(DAG.class);
        when(dag.getTargets()).thenReturn(List.of((Node) sourceNode));
        task.setDag(dag);

        TaskDto taskOwner = new TaskDto();
        taskOwner.setUserId(userId);
        when(taskService.findByTaskId(eq(taskId), anyString())).thenReturn(taskOwner);
        when(dataSourceService.findInfoByConnectionIdList(eq(List.of(connectionId)), eq(user), any(String[].class)))
                .thenReturn(List.of(sourceConnection(connectionId, "heartbeat-agent")));
        when(agentGroupService.getProcessNodeListWithGroup(any(TaskDto.class), eq(user)))
                .thenReturn(List.of("heartbeat-agent"));
        Worker sourceAgent = new Worker();
        sourceAgent.setProcessId("heartbeat-agent");
        when(workerService.findAvailableAgentByAccessNode(eq(user), anyList())).thenReturn(List.of(sourceAgent));
        when(workerService.findByProcessId(anyString(), eq(user), any(String[].class))).thenReturn(new WorkerDto());
        when(workerService.getLimitTaskNum(any(WorkerDto.class), eq(user))).thenReturn(10);
        when(taskService.runningTaskNum(anyString(), eq(user))).thenReturn(0);
        when(taskService.subCronOrPlanNum(any(TaskDto.class), eq(0))).thenReturn(0);

        CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
        calculationEngineVo.setProcessId("heartbeat-agent");
        calculationEngineVo.setRunningNum(0);
        calculationEngineVo.setTaskLimit(10);
        when(workerService.scheduleTaskToEngineWithStrictAgent(any(SchedulableDto.class), eq(user), eq("task"), eq("heartbeat")))
                .thenReturn(calculationEngineVo);

        service.cloudTaskLimitNum(task, user, true);

        org.mockito.ArgumentCaptor<SchedulableDto> scheduledTask = org.mockito.ArgumentCaptor.forClass(SchedulableDto.class);
        verify(workerService).scheduleTaskToEngineWithStrictAgent(scheduledTask.capture(), eq(user), eq("task"), eq("heartbeat"));
        TaskDto scheduledTaskDto = (TaskDto) scheduledTask.getValue();
        assertEquals(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), scheduledTaskDto.getAccessNodeType());
        assertEquals("heartbeat-agent", scheduledTaskDto.getAccessNodeProcessId());
        assertEquals(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name(), task.getAccessNodeType());
        assertNull(task.getAccessNodeProcessId());
        assertEquals("heartbeat-agent", task.getAgentId());
    }

    private DataSourceConnectionDto sourceConnection(String connectionId, String processId) {
        return sourceConnection(connectionId, processId, connectionId);
    }

    private DataSourceConnectionDto sourceConnection(String connectionId, String processId, String name) {
        DataSourceConnectionDto connection = new DataSourceConnectionDto();
        connection.setId(new ObjectId(connectionId));
        connection.setName(name);
        connection.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
        connection.setAccessNodeProcessId(processId);
        return connection;
    }
}
