package com.tapdata.tm.schedule.service.impl;

import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.service.TaskCollectionObjService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.impl.TaskScheduleServiceImpl;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TaskScheduleServiceImplTest {
    @Nested
    class SchedulingTest {
        final UserDetail user = new UserDetail("6393f084c162f518b18165c3", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        final ObjectId taskId = MongoUtils.toObjectId("6324562fc5c0a4052d821d90");
        TaskScheduleServiceImpl taskScheduleService;
        TaskService taskService;
        StateMachineService stateMachineService;
        MonitoringLogsService monitoringLogsService;
        TaskCollectionObjService taskCollectionObjService;
        MessageQueueService messageQueueService;
        TaskDto taskDto;

        @BeforeEach
        void beforeEach() {
            taskScheduleService = spy(new TaskScheduleServiceImpl());
            taskService = mock(TaskService.class);
            stateMachineService = mock(StateMachineService.class);
            monitoringLogsService = mock(MonitoringLogsService.class);
            taskCollectionObjService = mock(TaskCollectionObjService.class);
            messageQueueService = mock(MessageQueueService.class);

            taskScheduleService.setTaskService(taskService);
            taskScheduleService.setStateMachineService(stateMachineService);
            taskScheduleService.setMonitoringLogsService(monitoringLogsService);
            taskScheduleService.setTaskCollectionObjService(taskCollectionObjService);
            taskScheduleService.setMessageQueueService(messageQueueService);

            taskDto = new TaskDto();
            taskDto.setId(taskId);
            taskDto.setUserId(user.getUserId());
            taskDto.setName("test");
            taskDto.setStatus(TaskDto.STATUS_SCHEDULING);
            taskDto.setAgentId("old-agent");
            taskDto.setCanOpenInspect(false);
        }

        @Test
        void shouldNotStopOldAgentWhenTaskRescheduled() {
            CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
            calculationEngineVo.setProcessId("new-agent");
            doAnswer(invocation -> {
                taskDto.setAgentId("new-agent");
                return calculationEngineVo;
            }).when(taskScheduleService).cloudTaskLimitNum(taskDto, user, false);
            when(stateMachineService.executeAboutTask(taskDto, DataFlowEvent.SCHEDULE_SUCCESS, user)).thenReturn(StateMachineResult.ok());
            when(taskService.findById(taskId, user)).thenReturn(taskDto);
            doNothing().when(taskScheduleService).sendStartMsg(taskId.toHexString(), "new-agent", user);

            taskScheduleService.scheduling(taskDto, user, true);

            verify(taskService, never()).sendStoppingMsg(taskId.toHexString(), "old-agent", user, false);
            verify(stateMachineService, times(1)).executeAboutTask(taskDto, DataFlowEvent.SCHEDULE_SUCCESS, user);
            verify(taskScheduleService, times(1)).sendStartMsg(taskId.toHexString(), "new-agent", user);
        }
    }

    @Nested
    class testCloudTaskLimitNum {
        final UserDetail user = new UserDetail("6393f084c162f518b18165c3", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        final String processId = "632327dd287a904778c0a13c-1gd0l7dvk";
        TaskScheduleServiceImpl taskScheduleService = spy(TaskScheduleServiceImpl.class);
        TaskService taskService;
        UserService userService;
        WorkerService workerService;
        StateMachineService stateMachineService;
        TaskDto taskDto;
        WorkerDto workerDto;
        AgentGroupService agentGroupService;

        @BeforeEach
        void beforeEach() {
            taskService = mock(TaskService.class);
            userService = mock(UserService.class);
            workerService = mock(WorkerService.class);
            stateMachineService = mock(StateMachineService.class);
            agentGroupService = mock(AgentGroupService.class);
            taskScheduleService.setTaskService(taskService);
            taskScheduleService.setUserService(userService);
            taskScheduleService.setWorkerService(workerService);
            taskScheduleService.setStateMachineService(stateMachineService);
            taskDto = new TaskDto();
            taskDto.setUserId("6393f084c162f518b18165c3");
            taskDto.setName("test");
            taskDto.setAccessNodeProcessId("632327dd287a904778c0a13c-1gd0l7dvk");
            taskDto.setId(MongoUtils.toObjectId("6324562fc5c0a4052d821d90"));
            taskDto.setCrontabExpressionFlag(true);
            taskDto.setAccessNodeType("MANUALLY_SPECIFIED_BY_THE_USER");
            workerDto = new WorkerDto();
            workerDto.setProcessId(processId);
            workerDto.setAgentTags(null);
            workerDto.setUserId(user.getUserId());
            when(taskService.findByTaskId(taskDto.getId(), "user_id")).thenReturn(taskDto);
            when(userService.loadUserById(new ObjectId(taskDto.getUserId()))).thenReturn(user);
            ReflectionTestUtils.setField(taskScheduleService, "agentGroupService", agentGroupService);
        }

        @Test
        void testSpecifiedByTheUserExceedCloudTaskLimitNum() {
            when(workerService.findByProcessId(processId, user, "user_id", "agentTags", "process_id")).thenReturn(workerDto);
            when(workerService.getLimitTaskNum(workerDto, user)).thenReturn(2);
            when(taskService.runningTaskNum(processId, user)).thenReturn(4);
            when(taskService.subCronOrPlanNum(taskDto,4)).thenReturn(4);
            when(stateMachineService.executeAboutTask(taskDto, DataFlowEvent.SCHEDULE_FAILED, user)).thenReturn(StateMachineResult.ok());
            assertThrows(BizException.class, () -> taskScheduleService.cloudTaskLimitNum(taskDto, user, false));
        }

        @Test
        void testSpecifiedByTheUserNoExceedCloudTaskLimitNum() {
            when(workerService.findByProcessId(processId, user, "user_id", "agentTags", "process_id")).thenReturn(workerDto);
            when(workerService.getLimitTaskNum(workerDto, user)).thenReturn(2);
            when(taskService.runningTaskNum(processId, user)).thenReturn(3);
            when(taskService.subCronOrPlanNum(taskDto,3)).thenReturn(2);
            when(workerService.scheduleTaskToEngine(taskDto, user, "task", taskDto.getName())).thenReturn(new CalculationEngineVo());
            taskScheduleService.cloudTaskLimitNum(taskDto, user, false);
            verify(workerService, times(1)).scheduleTaskToEngine(taskDto, user, "task", taskDto.getName());
        }

        @Test
        void testNoExceedCloudTaskLimitNum() {
            when(workerService.findByProcessId(processId, user, "user_id", "agentTags", "process_id")).thenReturn(workerDto);
            when(workerService.getLimitTaskNum(workerDto, user)).thenReturn(2);
            CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
            calculationEngineVo.setRunningNum(1);
            calculationEngineVo.setTaskLimit(2);
            when(taskService.runningTaskNum(processId, user)).thenReturn(3);
            when(taskService.subCronOrPlanNum(taskDto,3)).thenReturn(2);
            when(workerService.scheduleTaskToEngine(taskDto, user, "task", taskDto.getName())).thenReturn(calculationEngineVo);
            when(taskService.subCronOrPlanNum(taskDto,1)).thenReturn(1);
            CalculationEngineVo result = taskScheduleService.cloudTaskLimitNum(taskDto, user, false);
            assertEquals(calculationEngineVo, result);
        }

        @Test
        void testExceedCloudTaskLimitNum() {
            when(workerService.findByProcessId(processId, user, "user_id", "agentTags", "process_id")).thenReturn(workerDto);
            when(workerService.getLimitTaskNum(workerDto, user)).thenReturn(2);
            CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
            calculationEngineVo.setRunningNum(4);
            calculationEngineVo.setTaskLimit(2);
            when(taskService.runningTaskNum(processId, user)).thenReturn(2);
            when(taskService.subCronOrPlanNum(taskDto,2)).thenReturn(1);
            when(workerService.scheduleTaskToEngine(taskDto, user, "task", taskDto.getName())).thenReturn(calculationEngineVo);
            when(stateMachineService.executeAboutTask(taskDto, DataFlowEvent.SCHEDULE_FAILED, user)).thenReturn(StateMachineResult.ok());
            when(taskService.subCronOrPlanNum(taskDto,4)).thenReturn(4);
            assertThrows(BizException.class, () -> taskScheduleService.cloudTaskLimitNum(taskDto, user, false));
        }
        @Test
        void testTaskIsGroupManually_main(){
            taskDto.setAccessNodeType("MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP");
            taskDto.setPriorityProcessId("worker_test2");
            CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
            List<Worker> availableAgent = new ArrayList<>();
            Worker worker1 = new Worker();
            worker1.setProcessId("worker_test1");
            Worker worker2 = new Worker();
            worker2.setProcessId("worker_test2");
            availableAgent.add(worker1);
            availableAgent.add(worker2);
            when(agentGroupService.getProcessNodeListWithGroup(taskDto, user)).thenReturn(Arrays.asList("worker_test1","worker_test2"));
            when(workerService.findAvailableAgentByAccessNode(any(),anyList())).thenReturn(availableAgent);
            when(workerService.scheduleTaskToEngine(taskDto, user, "task", taskDto.getName())).thenAnswer(invocationOnMock -> {
                TaskDto result = invocationOnMock.getArgument(0);
                Assertions.assertEquals("worker_test2",result.getAgentId());
                return calculationEngineVo;
            });
            taskScheduleService.cloudTaskLimitNum(taskDto, user, false);
        }
        @Test
        void testTaskIsGroupManually_PriorityProcessId_is_null(){
            taskDto.setAccessNodeType("MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP");
            CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
            List<Worker> availableAgent = new ArrayList<>();
            Worker worker1 = new Worker();
            worker1.setProcessId("worker_test1");
            Worker worker2 = new Worker();
            worker2.setProcessId("worker_test2");
            availableAgent.add(worker1);
            availableAgent.add(worker2);
            when(agentGroupService.getProcessNodeListWithGroup(taskDto, user)).thenReturn(Arrays.asList("worker_test1","worker_test2"));
            when(workerService.findAvailableAgentByAccessNode(any(),anyList())).thenReturn(availableAgent);
            when(workerService.scheduleTaskToEngine(taskDto, user, "task", taskDto.getName())).thenAnswer(invocationOnMock -> {
                TaskDto result = invocationOnMock.getArgument(0);
                Assertions.assertEquals("worker_test1",result.getAgentId());
                return calculationEngineVo;
            });
            taskScheduleService.cloudTaskLimitNum(taskDto, user, false);
        }
        @Test
        void testTaskIsGroupManually_accessNodeProcessIdList_is_Empty(){
            taskDto.setAccessNodeType("MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP");
            CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
            when(workerService.scheduleTaskToEngine(taskDto, user, "task", taskDto.getName())).thenAnswer(invocationOnMock -> {
                TaskDto result = invocationOnMock.getArgument(0);
                Assertions.assertNull(result.getAgentId());
                return calculationEngineVo;
            });
            taskScheduleService.cloudTaskLimitNum(taskDto, user, false);
        }

        @Test
        void testTaskIsGroupManually_availableAgent_is_Empty(){
            taskDto.setAccessNodeType("MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP");
            taskDto.setAgentId("offline-worker");
            when(agentGroupService.getProcessNodeListWithGroup(taskDto, user)).thenReturn(Arrays.asList("worker_test1","worker_test2"));
            when(workerService.findAvailableAgentByAccessNode(any(),anyList())).thenReturn(new ArrayList<>());

            CalculationEngineVo result = taskScheduleService.cloudTaskLimitNum(taskDto, user, false);

            Assertions.assertNull(taskDto.getAgentId());
            Assertions.assertEquals(0, result.getAvailable());
            Assertions.assertEquals(Integer.MAX_VALUE, result.getTaskLimit());
            verify(workerService, never()).scheduleTaskToEngine(taskDto, user, "task", taskDto.getName());
        }
    }
}
