package com.tapdata.tm.task.service;

import cn.hutool.extra.cglib.CglibUtil;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.MutiResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.bean.Task;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.utils.BeanUtil;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import io.jsonwebtoken.lang.Assert;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.beans.BeanUtils.*;

@ExtendWith(MockitoExtension.class)
public class TaskServiceTest {
    private TaskService taskService;

    @Test
    void testFindRunningTasksByAgentIdWithoutId() {
        TaskRepository repository = mock(TaskRepository.class);
        taskService = spy(new TaskService(repository));
        String processId = "  ";
        assertThrows(IllegalArgumentException.class, () -> taskService.findRunningTasksByAgentId(processId));
    }

    @Test
    void testFindRunningTasksByAgentIdWithId() {
        TaskRepository repository = mock(TaskRepository.class);
        taskService = spy(new TaskService(repository));
        String processId = "111";
        Query query = Query.query(Criteria.where("agentId").is(processId).and("status").is("running"));
        when(taskService.findAll(query)).thenReturn(new ArrayList<>());
        int actual = taskService.findRunningTasksByAgentId(processId);
        assertEquals(0, actual);
    }

    @Nested
    class TestCheckIsCronOrPlanTask {
        /**
         * 测试传入的Task任务为null
         */
        @Test
        void testCheckIsCronOrPlanTaskWithNullTask() {
            TaskRepository repository = mock(TaskRepository.class);
            taskService = spy(new TaskService(repository));
            assertThrows(IllegalArgumentException.class, () -> {
                taskService.checkIsCronOrPlanTask(null);
            });
        }

        /**
         * 测试传入的Task的crontabExpressionFlag属性为 null 的情况
         */
        @Test
        void testCheckIsCronOrPlanTaskWithNullCronTask() {
            TaskRepository repository = mock(TaskRepository.class);
            taskService = spy(new TaskService(repository));
            TaskDto taskDto = new TaskDto();
            boolean result = taskService.checkIsCronOrPlanTask(taskDto);
            assertEquals(false, result);
        }

        /**
         * 测试传入的Task的planStartDateFlag属性为true的情况
         */
        @Test
        void testCheckIsCronOrPlanTaskWithTruePlanTask() {
            TaskRepository repository = mock(TaskRepository.class);
            taskService = spy(new TaskService(repository));
            TaskDto taskDto = new TaskDto();
            taskDto.setPlanStartDateFlag(true);
            boolean result = taskService.checkIsCronOrPlanTask(taskDto);
            assertEquals(true, result);
        }

        /**
         * 测试传入的Task的planStartDateFlag属性为false的情况
         */
        @Test
        void testCheckIsCronOrPlanTaskWithFalsePlanTask() {
            TaskRepository repository = mock(TaskRepository.class);
            taskService = spy(new TaskService(repository));
            TaskDto taskDto = new TaskDto();
            taskDto.setPlanStartDateFlag(false);
            boolean result = taskService.checkIsCronOrPlanTask(taskDto);
            assertEquals(false, result);
        }

        /**
         * 测试传入的Task的crontabExpressionFlag属性为true的情况
         */
        @Test
        void testCheckIsCronOrPlanTaskWithTrueCronTask() {
            TaskRepository repository = mock(TaskRepository.class);
            taskService = spy(new TaskService(repository));
            TaskDto taskDto = new TaskDto();
            taskDto.setCrontabExpressionFlag(true);
            boolean result = taskService.checkIsCronOrPlanTask(taskDto);
            assertEquals(true, result);
        }

        /**
         * 测试传入的Task的crontabExpressionFlag属性为false的情况
         */
        @Test
        void testCheckIsCronOrPlanTaskWithFalseCronTask() {
            TaskRepository repository = mock(TaskRepository.class);
            taskService = spy(new TaskService(repository));
            TaskDto taskDto = new TaskDto();
            taskDto.setCrontabExpressionFlag(false);
            boolean result = taskService.checkIsCronOrPlanTask(taskDto);
            assertEquals(false, result);
        }
    }

    @Nested
    class TestBatchStart {
        final UserDetail user = new UserDetail("6393f084c162f518b18165c3", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        TaskRepository repository;
        ArrayList<TaskEntity> taskEntities = new ArrayList<>();
        SettingsService settingsService;
        List<ObjectId> ids;
        Query query;
        TaskEntity taskEntity;
        TaskScheduleService taskScheduleService;

        @BeforeEach
        void beforeEach() {
            repository = mock(TaskRepository.class);
            taskService = spy(new TaskService(repository));
            settingsService = mock(SettingsService.class);
            when(settingsService.isCloud()).thenReturn(true);
            taskService.setSettingsService(settingsService);
            ids = Arrays.asList("6324562fc5c0a4052d821d90").stream().map(ObjectId::new).collect(Collectors.toList());
            query = new Query(Criteria.where("_id").in(ids));
            taskEntity = new TaskEntity();
            taskEntity.setUserId("6393f084c162f518b18165c3");
            taskEntity.setAgentId("632327dd287a904778c0a13c-1gd0l7dvk");
            taskEntity.setName("test");
            taskEntity.setId(MongoUtils.toObjectId("6324562fc5c0a4052d821d90"));
            taskEntities.add(taskEntity);
            taskScheduleService = mock(TaskScheduleService.class);
            taskService.setTaskScheduleService(taskScheduleService);
            when(repository.findAll(query)).thenReturn(taskEntities);
        }

        @Test
        void testExceedBatchStart() {
            try (MockedStatic<DataPermissionHelper> dataPermissionHelperMockedStatic = mockStatic(DataPermissionHelper.class)) {
                Query query = new Query(Criteria.where("_id").is(taskEntity.getId()));
                query.fields().include("planStartDateFlag", "crontabExpressionFlag");
                when(repository.findOne(query)).thenReturn(Optional.ofNullable(taskEntity));
                List<TaskDto> taskDtos = CglibUtil.copyList(taskEntities, TaskDto::new);
                CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
                calculationEngineVo.setTaskLimit(2);
                calculationEngineVo.setRunningNum(2);
                calculationEngineVo.setTaskLimit(2);
                when(taskScheduleService.cloudTaskLimitNum(taskDtos.get(0), user, true)).thenReturn(calculationEngineVo);
                MonitoringLogsService monitoringLogsService = mock(MonitoringLogsService.class);
                taskService.setMonitoringLogsService(monitoringLogsService);
                List<MutiResponseMessage> mutiResponseMessages = taskService.batchStart(ids, user, null, null);
                assertEquals("Task.ScheduleLimit", mutiResponseMessages.get(0).getCode());
            }
        }

        @Test
        void testBatchStart() {
            try (MockedStatic<DataPermissionHelper> dataPermissionHelperMockedStatic = mockStatic(DataPermissionHelper.class)) {
                taskEntity.setCrontabExpressionFlag(true);
                Query query = new Query(Criteria.where("_id").is(taskEntity.getId()));
                query.fields().include("planStartDateFlag", "crontabExpressionFlag");
                when(repository.findOne(query)).thenReturn(Optional.ofNullable(taskEntity));
                List<TaskDto> taskDtos = CglibUtil.copyList(taskEntities, TaskDto::new);
                CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
                calculationEngineVo.setTaskLimit(2);
                calculationEngineVo.setRunningNum(2);
                calculationEngineVo.setTaskLimit(2);
                calculationEngineVo.setTotalLimit(2);
                when(taskScheduleService.cloudTaskLimitNum(taskDtos.get(0), user, true)).thenReturn(calculationEngineVo);
                MonitoringLogsService monitoringLogsService = mock(MonitoringLogsService.class);
                taskService.setMonitoringLogsService(monitoringLogsService);
                taskService.batchStart(ids, user, null, null);
                verify(taskService, times(1)).start(taskDtos.get(0), user, "11");
            }
        }
    }

    @Nested
    class TestRun {
        final UserDetail user = new UserDetail("6393f084c162f518b18165c3", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        TaskRepository repository;
        SettingsService settingsService;
        TaskEntity taskEntity;
        TaskScheduleService taskScheduleService;

        @BeforeEach
        void beforeEach() {
            repository=mock(TaskRepository.class);
            taskService = spy(new TaskService(repository));
            taskEntity=new TaskEntity();
            taskEntity.setUserId("6393f084c162f518b18165c3");
            taskEntity.setAgentId("632327dd287a904778c0a13c-1gd0l7dvk");
            taskEntity.setName("test");
            taskEntity.setId(MongoUtils.toObjectId("6324562fc5c0a4052d821d90"));
            settingsService=mock(SettingsService.class);
            when(settingsService.isCloud()).thenReturn(true);
            taskService.setSettingsService(settingsService);
            taskScheduleService=mock(TaskScheduleService.class);
            taskService.setTaskScheduleService(taskScheduleService);
        }

        @Test
        void testNoExceedRun() {
            try (MockedStatic<DataPermissionHelper> dataPermissionHelperMockedStatic = mockStatic(DataPermissionHelper.class)) {
                taskEntity.setCrontabExpressionFlag(true);
                TaskDto taskDto = new TaskDto();
                copyProperties(taskEntity, taskDto, TaskDto.class);
                Query query = new Query(Criteria.where("_id").is(taskDto.getId()));
                query.fields().include("planStartDateFlag", "crontabExpressionFlag");
                when(repository.findOne(query)).thenReturn(Optional.ofNullable(taskEntity));
                CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
                calculationEngineVo.setTaskLimit(2);
                calculationEngineVo.setRunningNum(2);
                calculationEngineVo.setTaskLimit(2);
                calculationEngineVo.setTotalLimit(2);
                when(taskScheduleService.cloudTaskLimitNum(taskDto, user, true)).thenReturn(calculationEngineVo);
                StateMachineService stateMachineService = mock(StateMachineService.class);
                taskService.setStateMachineService(stateMachineService);
                when(stateMachineService.executeAboutTask(taskDto, DataFlowEvent.START, user)).thenReturn(StateMachineResult.ok());
                taskService.run(taskDto, user);
                verify(stateMachineService, times(1)).executeAboutTask(taskDto, DataFlowEvent.START, user);
            }
        }

        @Test
        void testExceedRun() {
            try (MockedStatic<DataPermissionHelper> dataPermissionHelperMockedStatic = mockStatic(DataPermissionHelper.class)) {
                TaskDto taskDto = new TaskDto();
                copyProperties(taskEntity, taskDto, TaskDto.class);
                Query query = new Query(Criteria.where("_id").is(taskDto.getId()));
                query.fields().include("planStartDateFlag", "crontabExpressionFlag");
                when(repository.findOne(query)).thenReturn(Optional.ofNullable(taskEntity));
                TaskScheduleService taskScheduleService = mock(TaskScheduleService.class);
                taskService.setTaskScheduleService(taskScheduleService);
                CalculationEngineVo calculationEngineVo = new CalculationEngineVo();
                calculationEngineVo.setTaskLimit(2);
                calculationEngineVo.setRunningNum(2);
                calculationEngineVo.setTaskLimit(2);
                calculationEngineVo.setTotalLimit(2);
                when(taskScheduleService.cloudTaskLimitNum(taskDto, user, true)).thenReturn(calculationEngineVo);
                StateMachineService stateMachineService = mock(StateMachineService.class);
                taskService.setStateMachineService(stateMachineService);
                assertThrows(BizException.class, () -> taskService.run(taskDto, user));
            }
        }
    }
    @Nested
    class TestSubCronOrPlanNum{
        TaskRepository taskRepository;
        TaskEntity taskEntity;
        TaskDto taskDto;
        @BeforeEach
        void beforeEach(){
            taskEntity=new TaskEntity();
            taskEntity.setId(MongoUtils.toObjectId("6324562fc5c0a4052d821d90"));

            taskDto=new TaskDto();
            BeanUtils.copyProperties(taskEntity,taskDto);
            taskRepository = mock(TaskRepository.class);
            taskService=spy(new TaskService(taskRepository));
            Query query = new Query(Criteria.where("_id").is(taskDto.getId()));
            query.fields().include("planStartDateFlag", "crontabExpressionFlag");
            when(taskRepository.findOne(query)).thenReturn(Optional.ofNullable(taskEntity));
        }
        @DisplayName("test cron task sub 1")
        @Test
        void testSubCronOrPlanNum(){
            taskEntity.setCrontabExpressionFlag(true);
            try (MockedStatic<DataPermissionHelper> dataPermissionHelperMockedStatic = mockStatic(DataPermissionHelper.class)) {
                int result = taskService.subCronOrPlanNum(taskDto, 3);
                assertEquals(2,result);
            }
        }
        @DisplayName("test not cron task don't sub 1")
        @Test
        void testNoCronOrPlanTask(){
            taskEntity.setCrontabExpressionFlag(null);
            try (MockedStatic<DataPermissionHelper> dataPermissionHelperMockedStatic = mockStatic(DataPermissionHelper.class)) {
                int result = taskService.subCronOrPlanNum(taskDto, 3);
                assertEquals(3,result);
            }
        }
    }
}
