package com.tapdata.tm.schedule.service;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskRecordService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.TransformSchemaService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.internal.verification.Times;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ScheduleServiceTest {
    private ScheduleService scheduleService = new ScheduleService();
    private TaskRecordService taskRecordService;
    private TransformSchemaService transformSchema;
    @BeforeEach
    void beforeEach(){
        taskRecordService = mock(TaskRecordService.class);
        transformSchema = mock(TransformSchemaService.class);
        ReflectionTestUtils.setField(scheduleService, "taskRecordService", taskRecordService);
        ReflectionTestUtils.setField(scheduleService, "transformSchema", transformSchema);
    }
    @Nested
    class TestExecuteTask{
        final UserDetail user = new UserDetail("6393f084c162f518b18165c3", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        private TaskDto taskDto;
        @BeforeEach
        void beforeEach(){
            taskDto = new TaskDto();
            taskDto.setId(mock(ObjectId.class));
            taskDto.setUserId("6393f084c162f518b18165c3");
            taskDto.setName("test");
            taskDto.setPlanStartDateFlag(true);
        }
        @Test
        void testExecuteTaskNotExceed(){
            final CalculationEngineVo calculationEngineVo=new CalculationEngineVo();
            calculationEngineVo.setProcessId("632327dd287a904778c0a13c-1gd0l7dvk");
            calculationEngineVo.setTaskLimit(2);
            calculationEngineVo.setRunningNum(2);
            UserService userService = mock(UserService.class);
            WorkerService workerService = mock(WorkerService.class);
            TaskService taskService = mock(TaskService.class);
            try(MockedStatic<SpringContextHelper> springContextHelperMockedStatic = mockStatic(SpringContextHelper.class);){
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
            try(MockedStatic<SpringContextHelper> springContextHelperMockedStatic = mockStatic(SpringContextHelper.class)){
                taskDto.setAgentId("632327dd287a904778c0a13c-1gd0l7dvk");
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
        @Test
        void testExecuteTaskForInitial(){
            scheduleService = spy(scheduleService);
            final CalculationEngineVo calculationEngineVo=new CalculationEngineVo();
            calculationEngineVo.setProcessId("632327dd287a904778c0a13c-1gd0l7dvk");
            calculationEngineVo.setTaskLimit(2);
            calculationEngineVo.setRunningNum(3);
            UserService userService = mock(UserService.class);
            WorkerService workerService = mock(WorkerService.class);
            TaskService taskService = mock(TaskService.class);
            try(MockedStatic<SpringContextHelper> springContextHelperMockedStatic = mockStatic(SpringContextHelper.class);){
                taskDto.setAgentId("632327dd287a904778c0a13c-1gd0l7dvk");
                taskDto.setType("initial_sync");
                taskDto.setCrontabExpression("0 */1 * * * ?");
                taskDto.setCrontabExpressionFlag(true);
                taskDto.setStatus(TaskDto.STATUS_COMPLETE);
                taskDto.setScheduleDate(1724136420000L);
                springContextHelperMockedStatic.when(()->SpringContextHelper.getBean(eq(UserService.class))).thenReturn(userService);
                springContextHelperMockedStatic.when(()->SpringContextHelper.getBean(eq(WorkerService.class))).thenReturn(workerService);
                springContextHelperMockedStatic.when(()->SpringContextHelper.getBean(eq(TaskService.class))).thenReturn(taskService);
                when(userService.loadUserById(MongoUtils.toObjectId("6393f084c162f518b18165c3"))).thenReturn(user);
                when(workerService.scheduleTaskToEngine(taskDto, user, "task", taskDto.getName())).thenReturn(calculationEngineVo);
                scheduleService.executeTask(taskDto);
                verify(scheduleService, new Times(1)).createTaskRecordForInitial(taskDto);
                verify(taskService, new Times(1)).save(taskDto, user);
            }
        }
    }

}
