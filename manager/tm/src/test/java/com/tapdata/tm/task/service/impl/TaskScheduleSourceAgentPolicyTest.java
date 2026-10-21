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
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
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
    void normalTaskReturnsOriginalTaskWithoutReadingSourceAgentPolicy() {
        TaskScheduleServiceImpl service = new TaskScheduleServiceImpl();
        DataSourceService dataSourceService = mock(DataSourceService.class);
        ReflectionTestUtils.setField(service, "dataSourceService", dataSourceService);

        TaskDto task = new TaskDto();
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);

        TaskDto result = ReflectionTestUtils.invokeMethod(service, "taskForConnectionAgentPolicy", task, mock(UserDetail.class));

        assertSame(task, result);
        verify(dataSourceService, never()).findAll(any(Query.class));
    }

    @Test
    void sourceAgentPolicyReturnsOriginalTaskWhenDataSourceServiceIsUnavailable() {
        TaskScheduleServiceImpl service = new TaskScheduleServiceImpl();
        TaskDto task = logCollectorTask(new ObjectId().toHexString());

        TaskDto result = ReflectionTestUtils.invokeMethod(service, "taskForConnectionAgentPolicy", task, mock(UserDetail.class));

        assertSame(task, result);
    }

    @Test
    void sourceAgentPolicyReturnsOriginalTaskWhenTaskHasNoSourceConnection() {
        TaskScheduleServiceImpl service = new TaskScheduleServiceImpl();
        DataSourceService dataSourceService = mock(DataSourceService.class);
        ReflectionTestUtils.setField(service, "dataSourceService", dataSourceService);
        TaskDto task = logCollectorTask();

        TaskDto result = ReflectionTestUtils.invokeMethod(service, "taskForConnectionAgentPolicy", task, mock(UserDetail.class));

        assertSame(task, result);
        verify(dataSourceService, never()).findAll(any(Query.class));
    }

    @Test
    void sourceAgentPolicyReturnsOriginalTaskWhenSourceConnectionCannotBeFound() {
        TaskScheduleServiceImpl service = new TaskScheduleServiceImpl();
        DataSourceService dataSourceService = mock(DataSourceService.class);
        ReflectionTestUtils.setField(service, "dataSourceService", dataSourceService);
        String connectionId = new ObjectId().toHexString();
        TaskDto task = logCollectorTask(connectionId);
        when(dataSourceService.findAll(any(Query.class)))
                .thenReturn(List.of());

        TaskDto result = ReflectionTestUtils.invokeMethod(service, "taskForConnectionAgentPolicy", task, mock(UserDetail.class));

        assertSame(task, result);
        verify(dataSourceService).findAll(any(Query.class));
    }

    @Test
    void inheritedSourceAgentPolicyCopiesConnectionPolicyAndClearsStaleAgent() {
        TaskScheduleServiceImpl service = new TaskScheduleServiceImpl();
        DataSourceService dataSourceService = mock(DataSourceService.class);
        WorkerService workerService = mock(WorkerService.class);
        ReflectionTestUtils.setField(service, "dataSourceService", dataSourceService);
        ReflectionTestUtils.setField(service, "workerService", workerService);
        String connectionId = new ObjectId().toHexString();
        TaskDto task = logCollectorTask(connectionId);
        task.setAgentId("stale-agent");
        task.setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
        task.setAttrs(Map.of("sourceAgentPolicyInherited", true));
        when(dataSourceService.findAll(any(Query.class)))
                .thenReturn(List.of(sourceConnection(connectionId, "source-agent")));
        Worker available = new Worker();
        available.setProcessId("source-agent");
        when(workerService.findAvailableAgentByAccessNode(any(UserDetail.class), anyList())).thenReturn(List.of(available));

        TaskDto result = ReflectionTestUtils.invokeMethod(service, "taskForConnectionAgentPolicy", task, mock(UserDetail.class));

        assertNotSame(task, result);
        assertEquals(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), result.getAccessNodeType());
        assertEquals("source-agent", result.getAccessNodeProcessId());
        assertNull(result.getAgentId());
        assertEquals(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name(), task.getAccessNodeType());
        assertEquals("stale-agent", task.getAgentId());
    }

    @Test
    void explicitTaskAgentPolicyOverrideUsesOnlyAnAvailableAllowedAgent() {
        TaskScheduleServiceImpl service = new TaskScheduleServiceImpl();
        DataSourceService dataSourceService = mock(DataSourceService.class);
        ReflectionTestUtils.setField(service, "dataSourceService", dataSourceService);
        WorkerService workerService = mock(WorkerService.class);
        ReflectionTestUtils.setField(service, "workerService", workerService);
        Worker worker = new Worker();
        worker.setProcessId("task-agent");
        when(workerService.findAvailableAgentByAccessNode(any(UserDetail.class), anyList())).thenReturn(List.of(worker));
        String connectionId = new ObjectId().toHexString();
        TaskDto task = logCollectorTask(connectionId);
        task.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
        task.setAccessNodeProcessId("task-agent");
        when(dataSourceService.findAll(any(Query.class)))
                .thenReturn(List.of(sourceConnection(connectionId, "task-agent")));

        TaskDto result = ReflectionTestUtils.invokeMethod(service, "taskForConnectionAgentPolicy", task, mock(UserDetail.class));

        assertNotSame(task, result);
        assertEquals("task-agent", result.getAccessNodeProcessId());
        verify(dataSourceService).findAll(any(Query.class));
    }

    @Test
    void conflictingInheritedSourceAgentPoliciesThrowSourceAgentConflict() {
        TaskScheduleServiceImpl service = new TaskScheduleServiceImpl();
        DataSourceService dataSourceService = mock(DataSourceService.class);
        ReflectionTestUtils.setField(service, "dataSourceService", dataSourceService);
        String firstConnectionId = new ObjectId().toHexString();
        String secondConnectionId = new ObjectId().toHexString();
        TaskDto task = logCollectorTask(firstConnectionId, secondConnectionId);
        when(dataSourceService.findAll(any(Query.class)))
                .thenReturn(List.of(sourceConnection(firstConnectionId, "agent-a", "orders"),
                        sourceConnection(secondConnectionId, "agent-b", "users")));

        BizException exception = assertThrows(BizException.class,
                () -> ReflectionTestUtils.invokeMethod(service, "taskForConnectionAgentPolicy", task, mock(UserDetail.class)));

        assertEquals("Task.SourceAgentConflict", exception.getErrorCode());
        assertTrue(exception.getMessage().contains("orders"));
        assertTrue(exception.getMessage().contains("users"));
    }

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
        when(dataSourceService.findAll(any(Query.class)))
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
    void sharedMiningTaskExplicitAgentPolicyOverridesSourceConnection() {
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
        task.setName("shared-override");
        task.setSyncType(TaskDto.SYNC_TYPE_LOG_COLLECTOR);
        task.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
        task.setAccessNodeProcessId("task-agent");
        task.setAccessNodeProcessIdList(List.of("task-agent"));
        task.setAgentId("old-agent");

        LogCollectorNode logCollectorNode = new LogCollectorNode();
        logCollectorNode.setConnectionIds(List.of(connectionId));
        DAG dag = mock(DAG.class);
        when(dag.getSources()).thenReturn(List.of((Node) logCollectorNode));
        task.setDag(dag);

        TaskDto taskOwner = new TaskDto();
        taskOwner.setUserId(userId);
        when(taskService.findByTaskId(eq(taskId), anyString())).thenReturn(taskOwner);
        when(dataSourceService.findAll(any(Query.class)))
                .thenReturn(List.of(sourceConnection(connectionId, "task-agent")));
        when(agentGroupService.getProcessNodeListWithGroup(any(TaskDto.class), eq(user)))
                .thenReturn(List.of("task-agent"));
        Worker taskAgent = new Worker();
        taskAgent.setProcessId("task-agent");
        when(workerService.findAvailableAgentByAccessNode(eq(user), anyList())).thenReturn(List.of(taskAgent));
        when(workerService.findByProcessId(anyString(), eq(user), any(String[].class))).thenReturn(new WorkerDto());
        when(workerService.getLimitTaskNum(any(WorkerDto.class), eq(user))).thenReturn(10);
        when(taskService.runningTaskNum(anyString(), eq(user))).thenReturn(0);
        when(taskService.subCronOrPlanNum(any(TaskDto.class), eq(0))).thenReturn(0);

        CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
        calculationEngineVo.setProcessId("task-agent");
        calculationEngineVo.setRunningNum(0);
        calculationEngineVo.setTaskLimit(10);
        when(workerService.scheduleTaskToEngineWithStrictAgent(any(SchedulableDto.class), eq(user), eq("task"), eq("shared-override")))
                .thenReturn(calculationEngineVo);

        service.cloudTaskLimitNum(task, user, true);

        org.mockito.ArgumentCaptor<SchedulableDto> scheduledTask = org.mockito.ArgumentCaptor.forClass(SchedulableDto.class);
        verify(workerService).scheduleTaskToEngineWithStrictAgent(scheduledTask.capture(), eq(user), eq("task"), eq("shared-override"));
        TaskDto scheduledTaskDto = (TaskDto) scheduledTask.getValue();
        assertEquals(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), scheduledTaskDto.getAccessNodeType());
        assertEquals("task-agent", scheduledTaskDto.getAccessNodeProcessId());
        verify(dataSourceService).findAll(any(Query.class));
        assertEquals("task-agent", task.getAgentId());
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
        when(dataSourceService.findAll(any(Query.class)))
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
        verify(workerService, never()).scheduleTaskToEngineWithStrictAgent(any(SchedulableDto.class), eq(user), eq("task"), eq("shared"));
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
        when(dataSourceService.findAll(any(Query.class)))
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
        when(dataSourceService.findAll(any(Query.class)))
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

        verify(dataSourceService, never()).findAll(any(Query.class));
    }

    @Test
    void normalTaskUsesIntersectionOfAllConnectionAgentPolicies() {
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
        String sourceId = new ObjectId().toHexString();
        String targetId = new ObjectId().toHexString();
        TaskDto task = new TaskDto();
        task.setId(taskId);
        task.setUserId(userId);
        task.setName("sync-intersection");
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        task.setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());

        TableNode source = new TableNode();
        source.setConnectionId(sourceId);
        TableNode target = new TableNode();
        target.setConnectionId(targetId);
        DAG dag = mock(DAG.class);
        when(dag.getNodes()).thenReturn(List.of(source, target));
        task.setDag(dag);

        TaskDto taskOwner = new TaskDto();
        taskOwner.setUserId(userId);
        when(taskService.findByTaskId(eq(taskId), anyString())).thenReturn(taskOwner);
        when(dataSourceService.findAll(any(Query.class)))
                .thenReturn(List.of(sourceConnection(sourceId, "agent-a"),
                        sourceConnection(targetId, "agent-a")));
        Worker agent = new Worker();
        agent.setProcessId("agent-a");
        when(workerService.findAvailableAgentByAccessNode(eq(user), eq(List.of("agent-a"))))
                .thenReturn(List.of(agent));

        CalculationEngineVo result = new CalculationEngineVo();
        result.setProcessId("agent-a");
        result.setRunningNum(0);
        result.setTaskLimit(10);
        when(workerService.scheduleTaskToEngineWithStrictAgent(any(SchedulableDto.class), eq(user), eq("task"), eq("sync-intersection")))
                .thenReturn(result);

        service.cloudTaskLimitNum(task, user, true);

        org.mockito.ArgumentCaptor<SchedulableDto> scheduledTask = org.mockito.ArgumentCaptor.forClass(SchedulableDto.class);
        verify(workerService).scheduleTaskToEngineWithStrictAgent(scheduledTask.capture(), eq(user), eq("task"), eq("sync-intersection"));
        TaskDto scheduledTaskDto = (TaskDto) scheduledTask.getValue();
        assertEquals(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), scheduledTaskDto.getAccessNodeType());
        assertEquals("agent-a", scheduledTaskDto.getAccessNodeProcessId());
        assertEquals(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name(), task.getAccessNodeType());
        assertEquals("agent-a", task.getAgentId());
    }

    @Test
    void saveValidationRejectsEmptyConnectionAgentIntersection() {
        TaskScheduleServiceImpl service = new TaskScheduleServiceImpl();
        DataSourceService dataSourceService = mock(DataSourceService.class);
        AgentGroupService agentGroupService = mock(AgentGroupService.class);
        ReflectionTestUtils.setField(service, "dataSourceService", dataSourceService);
        ReflectionTestUtils.setField(service, "agentGroupService", agentGroupService);

        String firstConnectionId = new ObjectId().toHexString();
        String secondConnectionId = new ObjectId().toHexString();
        TaskDto task = new TaskDto();
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        task.setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
        TableNode first = new TableNode();
        first.setConnectionId(firstConnectionId);
        TableNode second = new TableNode();
        second.setConnectionId(secondConnectionId);
        DAG dag = mock(DAG.class);
        when(dag.getNodes()).thenReturn(List.of(first, second));
        task.setDag(dag);
        when(dataSourceService.findAll(any(Query.class)))
                .thenReturn(List.of(sourceConnection(firstConnectionId, "agent-a"),
                        sourceConnection(secondConnectionId, "agent-b")));

        BizException exception = assertThrows(BizException.class,
                () -> service.validateConnectionAgentPolicy(task, mock(UserDetail.class)));

        assertEquals("Task.SourceAgentConflict", exception.getErrorCode());
    }

    @Test
    void saveValidationRejectsTaskAgentGroupOutsideConnectionAgentGroup() {
        TaskScheduleServiceImpl service = new TaskScheduleServiceImpl();
        DataSourceService dataSourceService = mock(DataSourceService.class);
        AgentGroupService agentGroupService = mock(AgentGroupService.class);
        ReflectionTestUtils.setField(service, "dataSourceService", dataSourceService);
        ReflectionTestUtils.setField(service, "agentGroupService", agentGroupService);

        String connectionId = new ObjectId().toHexString();
        TaskDto task = new TaskDto();
        task.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        task.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
        task.setAccessNodeProcessId("group-b");
        TableNode source = new TableNode();
        source.setConnectionId(connectionId);
        DAG dag = mock(DAG.class);
        when(dag.getNodes()).thenReturn(List.of(source));
        task.setDag(dag);

        DataSourceConnectionDto connection = new DataSourceConnectionDto();
        connection.setId(new ObjectId(connectionId));
        connection.setName("source");
        connection.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
        connection.setAccessNodeProcessId("group-a");
        when(dataSourceService.findAll(any(Query.class)))
                .thenReturn(List.of(connection));
        UserDetail user = mock(UserDetail.class);
        when(agentGroupService.getProcessNodeListWithGroup(eq(connection), eq(user)))
                .thenReturn(List.of("agent-1"));
        when(agentGroupService.getProcessNodeListWithGroup(eq(task), eq(user)))
                .thenReturn(List.of("agent-2"));

        BizException exception = assertThrows(BizException.class,
                () -> service.validateConnectionAgentPolicy(task, user));

        assertEquals("Task.AgentConflict", exception.getErrorCode());
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
        when(dataSourceService.findAll(any(Query.class)))
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

    @Test
    void deletedOrEmptyConnectionGroupReportsTheConnectionConflict() {
        for (String code : List.of("group.not.fund", "group.agent.not.available")) {
            PolicyFixture f = new PolicyFixture();
            f.connection.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
            when(f.groups.getProcessNodeListWithGroup(f.connection, f.user)).thenThrow(new BizException(code));
            BizException error = assertThrows(BizException.class, () -> f.service.validateConnectionAgentPolicy(f.task, f.user));
            assertEquals("Task.SourceAgentConflict", error.getErrorCode());
            assertTrue(error.getMessage().contains("source"));
        }
    }

    @Test
    void unrelatedGroupFailureIsNotHiddenAsAnAgentConflict() {
        PolicyFixture f = new PolicyFixture();
        f.connection.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
        BizException failure = new BizException("SystemError");
        when(f.groups.getProcessNodeListWithGroup(f.connection, f.user)).thenThrow(failure);
        BizException error = assertThrows(BizException.class, () -> f.service.validateConnectionAgentPolicy(f.task, f.user));
        assertSame(failure, error);
    }

    @Test
    void blankConnectionAgentDoesNotRestrictTheTask() {
        PolicyFixture f = new PolicyFixture();
        f.connection.setAccessNodeProcessId(" ");
        f.service.validateConnectionAgentPolicy(f.task, f.user);
        assertSame(f.task, f.runtimeTask());
        verify(f.workers, never()).findAvailableAgentByAccessNode(any(), anyList());
    }

    @Test
    void connectionAndTaskGroupsAreResolvedUnderTheirOwnOwners() {
        PolicyFixture f = new PolicyFixture();
        UserService users = mock(UserService.class);
        SettingsService settings = mock(SettingsService.class);
        f.service.setUserService(users);
        f.service.setSettingsService(settings);
        when(settings.isCloud()).thenReturn(true);
        String connectionOwnerId = new ObjectId().toHexString();
        String taskOwnerId = new ObjectId().toHexString();
        UserDetail connectionOwner = mock(UserDetail.class);
        UserDetail taskOwner = mock(UserDetail.class);
        when(users.loadUserById(new ObjectId(connectionOwnerId))).thenReturn(connectionOwner);
        when(users.loadUserById(new ObjectId(taskOwnerId))).thenReturn(taskOwner);
        f.connection.setUserId(connectionOwnerId);
        f.connection.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
        f.task.setUserId(taskOwnerId);
        f.task.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
        f.task.setAccessNodeProcessId("task-group");
        when(f.groups.getProcessNodeListWithGroup(f.connection, connectionOwner)).thenReturn(List.of("agent-a"));
        when(f.groups.getProcessNodeListWithGroup(f.task, taskOwner)).thenReturn(List.of("agent-a"));
        f.service.validateConnectionAgentPolicy(f.task, f.user);
        verify(f.groups).getProcessNodeListWithGroup(f.connection, connectionOwner);
        verify(f.groups).getProcessNodeListWithGroup(f.task, taskOwner);
        verify(f.groups, never()).getProcessNodeListWithGroup(f.connection, f.user);
    }

    @Test
    void connectionPriorityAndStableFallbackIgnoreDatabaseOrder() {
        PolicyFixture f = new PolicyFixture();
        f.connection.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
        when(f.groups.getProcessNodeListWithGroup(f.connection, f.user)).thenReturn(List.of("agent-b", "agent-c", "agent-a"));
        f.available("agent-c", "agent-b", "agent-a");
        f.connection.setPriorityProcessId("agent-b");
        assertEquals("agent-b", f.runtimeTask().getAccessNodeProcessId());
        f.task.setPriorityProcessId("agent-c");
        assertEquals("agent-c", f.runtimeTask().getAccessNodeProcessId());
        f.task.setPriorityProcessId(null);
        f.connection.setPriorityProcessId("unavailable");
        assertEquals("agent-a", f.runtimeTask().getAccessNodeProcessId());
        f.available("agent-b", "agent-a", "agent-c");
        assertEquals("agent-a", f.runtimeTask().getAccessNodeProcessId());
    }

    @Test
    void unavailableOrExcludedCandidatesNeverReachStrictScheduling() {
        PolicyFixture f = new PolicyFixture();
        f.available();
        assertNull(f.runtimeTask());
        verify(f.workers, never()).scheduleTaskToEngineWithStrictAgent(any(), any(), anyString(), anyString());
    }

    @Test
    void missingConnectionDoesNotDisableRestrictionsOnRemainingConnections() {
        PolicyFixture f = new PolicyFixture();
        String missingId = new ObjectId().toHexString();
        f.task.setDag(logCollectorTask(f.connection.getId().toHexString(), missingId).getDag());
        f.task.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
        f.task.setAccessNodeProcessId("agent-b");
        BizException error = assertThrows(BizException.class, () -> f.service.validateConnectionAgentPolicy(f.task, f.user));
        assertEquals("Task.AgentConflict", error.getErrorCode());
    }

    @Test
    void clientInheritedFlagCannotBypassAnExplicitTaskAgent() {
        PolicyFixture f = new PolicyFixture();
        f.task.setAttrs(Map.of("sourceAgentPolicyInherited", true));
        f.task.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
        f.task.setAccessNodeProcessId("agent-b");
        BizException error = assertThrows(BizException.class, () -> f.service.validateConnectionAgentPolicy(f.task, f.user));
        assertEquals("Task.AgentConflict", error.getErrorCode());
    }

    @Test
    void inheritedFlagDoesNotReplaceManualPolicyWhenConnectionsAreMissing() {
        PolicyFixture f = new PolicyFixture();
        when(f.sources.findAll(any(Query.class))).thenReturn(List.of());
        f.task.setAttrs(Map.of("sourceAgentPolicyInherited", true));
        f.task.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name());
        f.task.setAccessNodeProcessId("agent-b");
        assertSame(f.task, f.runtimeTask());
        assertEquals("agent-b", f.task.getAccessNodeProcessId());
    }

    @Test
    void sharedCacheKeepsTheAgentConflictExemption() {
        PolicyFixture f = new PolicyFixture();
        f.task.setShareCache(true);
        f.service.validateConnectionAgentPolicy(f.task, f.user);
        assertSame(f.task, f.runtimeTask());
        verify(f.sources, never()).findAll(any(Query.class));
    }

    @Test
    void onlyAgentPolicyFieldsAreLoadedForAllConnectionIds() {
        PolicyFixture f = new PolicyFixture();
        f.service.validateConnectionAgentPolicy(f.task, f.user);
        org.mockito.ArgumentCaptor<Query> query = org.mockito.ArgumentCaptor.forClass(Query.class);
        verify(f.sources).findAll(query.capture());
        assertEquals(java.util.Set.of("_id", "name", "accessNodeType", "accessNodeProcessId", "priorityProcessId", "user_id"),
                query.getValue().getFieldsObject().keySet());
        assertEquals(List.of(f.connection.getId().toHexString()),
                ((org.bson.Document) query.getValue().getQueryObject().get("_id")).get("$in"));
    }

    private class PolicyFixture {
        final TaskScheduleServiceImpl service = new TaskScheduleServiceImpl();
        final DataSourceService sources = mock(DataSourceService.class);
        final WorkerService workers = mock(WorkerService.class);
        final AgentGroupService groups = mock(AgentGroupService.class);
        final UserDetail user = mock(UserDetail.class);
        final DataSourceConnectionDto connection;
        final TaskDto task;

        PolicyFixture() {
            service.setDataSourceService(sources);
            service.setWorkerService(workers);
            service.setAgentGroupService(groups);
            String id = new ObjectId().toHexString();
            connection = sourceConnection(id, "agent-a", "source");
            task = logCollectorTask(id);
            when(sources.findAll(any(Query.class))).thenReturn(List.of(connection));
        }

        void available(String... ids) {
            List<Worker> available = new java.util.ArrayList<>();
            for (String id : ids) {
                Worker worker = new Worker();
                worker.setProcessId(id);
                available.add(worker);
            }
            when(workers.findAvailableAgentByAccessNode(eq(user), anyList())).thenReturn(available);
        }

        TaskDto runtimeTask() {
            return ReflectionTestUtils.invokeMethod(service, "taskForConnectionAgentPolicy", task, user);
        }
    }

    private TaskDto logCollectorTask(String... connectionIds) {
        TaskDto task = new TaskDto();
        task.setSyncType(TaskDto.SYNC_TYPE_LOG_COLLECTOR);
        task.setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());

        LogCollectorNode source = new LogCollectorNode();
        source.setConnectionIds(List.of(connectionIds));
        DAG dag = mock(DAG.class);
        when(dag.getSources()).thenReturn(List.of((Node) source));
        task.setDag(dag);
        return task;
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
