package com.tapdata.tm.schedule.service.impl;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.impl.TaskScheduleServiceImpl;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TaskScheduleServiceImplTest {
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

        @BeforeEach
        void beforeEach() {
            taskService = mock(TaskService.class);
            userService = mock(UserService.class);
            workerService = mock(WorkerService.class);
            stateMachineService = mock(StateMachineService.class);
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
            when(workerService.findByProcessId(processId, user, "user_id", "agentTags", "process_id")).thenReturn(workerDto);
            when(workerService.getLimitTaskNum(workerDto, user)).thenReturn(2);
        }

        @Test
        void testSpecifiedByTheUserExceedCloudTaskLimitNum() {
            when(taskService.runningTaskNum(processId, user)).thenReturn(4);
            when(taskService.subCronOrPlanNum(taskDto,4)).thenReturn(4);
            when(stateMachineService.executeAboutTask(taskDto, DataFlowEvent.SCHEDULE_FAILED, user)).thenReturn(StateMachineResult.ok());
            assertThrows(BizException.class, () -> taskScheduleService.cloudTaskLimitNum(taskDto, user, false));
        }

        @Test
        void testSpecifiedByTheUserNoExceedCloudTaskLimitNum() {
            when(taskService.runningTaskNum(processId, user)).thenReturn(3);
            when(taskService.subCronOrPlanNum(taskDto,3)).thenReturn(2);
            when(workerService.scheduleTaskToEngine(taskDto, user, "task", taskDto.getName())).thenReturn(new CalculationEngineVo());
            taskScheduleService.cloudTaskLimitNum(taskDto, user, false);
            verify(workerService, times(1)).scheduleTaskToEngine(taskDto, user, "task", taskDto.getName());
        }

        @Test
        void testNoExceedCloudTaskLimitNum() {
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
    }
}
