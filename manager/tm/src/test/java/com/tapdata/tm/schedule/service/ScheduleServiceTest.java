package com.tapdata.tm.schedule.service;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ScheduleServiceTest {
    @Nested
    class TestExecuteTask{
        final UserDetail user = new UserDetail("6393f084c162f518b18165c3", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        @Test
        void testExecuteTaskNotExceed(){
            final CalculationEngineVo calculationEngineVo=new CalculationEngineVo();
            calculationEngineVo.setProcessId("632327dd287a904778c0a13c-1gd0l7dvk");
            calculationEngineVo.setTaskLimit(2);
            calculationEngineVo.setRunningNum(2);
            UserService userService = mock(UserService.class);
            WorkerService workerService = mock(WorkerService.class);
            TaskService taskService = mock(TaskService.class);
            ScheduleService scheduleService=new ScheduleService();
            TaskDto taskDto=new TaskDto();
            try(MockedStatic<SpringContextHelper> springContextHelperMockedStatic = mockStatic(SpringContextHelper.class);){
                taskDto.setUserId("6393f084c162f518b18165c3");
                taskDto.setAgentId("632327dd287a904778c0a13c-1gd0l7dvk");
                taskDto.setName("test");
                taskDto.setPlanStartDateFlag(true);
                springContextHelperMockedStatic.when(()->SpringContextHelper.getBean(eq(UserService.class))).thenReturn(userService);
                springContextHelperMockedStatic.when(()->SpringContextHelper.getBean(eq(WorkerService.class))).thenReturn(workerService);
                springContextHelperMockedStatic.when(()->SpringContextHelper.getBean(eq(TaskService.class))).thenReturn(taskService);
                when(userService.loadUserById(MongoUtils.toObjectId("6393f084c162f518b18165c3"))).thenReturn(user);
                when(workerService.scheduleTaskToEngine(taskDto, user, "task", taskDto.getName())).thenReturn(calculationEngineVo);
                scheduleService.executeTask(taskDto);
            }
        }
        @Test
        void testExecuteTaskExceed(){
            final CalculationEngineVo calculationEngineVo=new CalculationEngineVo();
            calculationEngineVo.setProcessId("632327dd287a904778c0a13c-1gd0l7dvk");
            calculationEngineVo.setTaskLimit(2);
            calculationEngineVo.setRunningNum(3);
            UserService userService = mock(UserService.class);
            WorkerService workerService = mock(WorkerService.class);
            TaskService taskService = mock(TaskService.class);
            ScheduleService scheduleService=new ScheduleService();
            TaskDto taskDto=new TaskDto();
            try(MockedStatic<SpringContextHelper> springContextHelperMockedStatic = mockStatic(SpringContextHelper.class)){
                taskDto.setUserId("6393f084c162f518b18165c3");
                taskDto.setAgentId("632327dd287a904778c0a13c-1gd0l7dvk");
                taskDto.setName("test");
                springContextHelperMockedStatic.when(()->SpringContextHelper.getBean(eq(UserService.class))).thenReturn(userService);
                springContextHelperMockedStatic.when(()->SpringContextHelper.getBean(eq(WorkerService.class))).thenReturn(workerService);
                springContextHelperMockedStatic.when(()->SpringContextHelper.getBean(eq(TaskService.class))).thenReturn(taskService);
                when(userService.loadUserById(MongoUtils.toObjectId("6393f084c162f518b18165c3"))).thenReturn(user);
                when(workerService.scheduleTaskToEngine(taskDto, user, "task", taskDto.getName())).thenReturn(calculationEngineVo);
                when(taskService.subCronOrPlanNum(taskDto,3)).thenReturn(3);
                scheduleService.executeTask(taskDto);
                assertEquals("Task.ScheduleLimit",taskDto.getCrontabScheduleMsg());
            }
        }
    }

}
