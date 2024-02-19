package com.tapdata.tm.task.service;

import cn.hutool.extra.cglib.CglibUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.MutiResponseMessage;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.vo.TestRunDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.permissions.service.DataPermissionService;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.bean.Chart6Vo;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
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

    @Nested
    class TestCheckCloudTaskLimit {
        TaskRepository taskRepository = mock(TaskRepository.class);

        SettingsService settingsService = mock(SettingsService.class);

        TaskScheduleService taskScheduleService = mock(TaskScheduleService.class);
        WorkerService workerService = mock(WorkerService.class);

        final UserDetail user = new UserDetail("6393f084c162f518b18165c3", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        @BeforeEach
        void beforeEach() {
            taskService = new TaskService(taskRepository);
            taskService.setSettingsService(settingsService);
            taskService.setTaskScheduleService(taskScheduleService);
            ReflectionTestUtils.setField(taskService,"workerService",workerService);
        }
        @Test
        void test_isDass(){
            when(settingsService.isCloud()).thenReturn(false);
            ObjectId taskId = MongoUtils.toObjectId("632454d5287a904778c40f8d");
            boolean result = taskService.checkCloudTaskLimit(taskId,user,true);
            assertTrue(result);
        }

        @Test
        void test_isCloudLimit(){
            ObjectId taskId = MongoUtils.toObjectId("632454d5287a904778c40f8d");
            Query query = new Query(Criteria.where("_id").is(taskId));
            query.fields().include("id","agentId","agentTags");
            TaskEntity mockTask = new TaskEntity();
            mockTask.setCrontabExpressionFlag(false);
            mockTask.setId(MongoUtils.toObjectId("632454d5287a904778c40f8d"));
            try (MockedStatic<DataPermissionService> serviceMockedStatic = Mockito.mockStatic(DataPermissionService.class)) {
                serviceMockedStatic.when(DataPermissionService::isCloud).thenReturn(true);
                when(settingsService.isCloud()).thenReturn(true);
                when(taskRepository.findOne(query)).thenReturn(Optional.of(mockTask));
                TaskDto mockTaskDto = new TaskDto();
                BeanUtils.copyProperties(mockTask,mockTaskDto);
                CalculationEngineVo mockEngineVo = new CalculationEngineVo();
                mockEngineVo.setTaskLimit(5);
                mockEngineVo.setRunningNum(5);
                when(workerService.calculationEngine(mockTaskDto,user,null)).thenReturn(mockEngineVo);
                Query mockQuery = new Query(Criteria.where("_id").is(taskId));
                mockQuery.fields().include("planStartDateFlag", "crontabExpressionFlag");
                when(taskRepository.findOne(mockQuery)).thenReturn(Optional.of(mockTask));
                boolean result = taskService.checkCloudTaskLimit(taskId,user,true);
                assertFalse(result);
            }
        }

        @Test
        void test_isCloudLimitNotReached(){
            ObjectId taskId = MongoUtils.toObjectId("632454d5287a904778c40f8d");
            Query query = new Query(Criteria.where("_id").is(taskId));
            query.fields().include("id","agentId","agentTags");
            TaskEntity mockTask = new TaskEntity();
            mockTask.setCrontabExpressionFlag(false);
            mockTask.setId(MongoUtils.toObjectId("632454d5287a904778c40f8d"));
            try (MockedStatic<DataPermissionService> serviceMockedStatic = Mockito.mockStatic(DataPermissionService.class)) {
                serviceMockedStatic.when(DataPermissionService::isCloud).thenReturn(true);
                when(settingsService.isCloud()).thenReturn(true);
                when(taskRepository.findOne(query)).thenReturn(Optional.of(mockTask));
                TaskDto mockTaskDto = new TaskDto();
                BeanUtils.copyProperties(mockTask,mockTaskDto);
                CalculationEngineVo mockEngineVo = new CalculationEngineVo();
                mockEngineVo.setTaskLimit(5);
                mockEngineVo.setRunningNum(4);
                when(workerService.calculationEngine(mockTaskDto,user,null)).thenReturn(mockEngineVo);
                Query mockQuery = new Query(Criteria.where("_id").is(taskId));
                mockQuery.fields().include("planStartDateFlag", "crontabExpressionFlag");
                when(taskRepository.findOne(mockQuery)).thenReturn(Optional.of(mockTask));
                boolean result = taskService.checkCloudTaskLimit(taskId,user,true);
                assertTrue(result);
            }
        }

        @Test
        void test_isCloudLimitScheduling(){
            ObjectId taskId = MongoUtils.toObjectId("632454d5287a904778c40f8d");
            Query query = new Query(Criteria.where("_id").is(taskId));
            query.fields().include("id","agentId","agentTags");
            TaskEntity mockTask = new TaskEntity();
            mockTask.setCrontabExpressionFlag(true);
            mockTask.setId(MongoUtils.toObjectId("632454d5287a904778c40f8d"));
            try (MockedStatic<DataPermissionService> serviceMockedStatic = Mockito.mockStatic(DataPermissionService.class)) {
                serviceMockedStatic.when(DataPermissionService::isCloud).thenReturn(true);
                when(settingsService.isCloud()).thenReturn(true);
                when(taskRepository.findOne(query)).thenReturn(Optional.of(mockTask));
                TaskDto mockTaskDto = new TaskDto();
                BeanUtils.copyProperties(mockTask,mockTaskDto);
                CalculationEngineVo mockEngineVo = new CalculationEngineVo();
                mockEngineVo.setTaskLimit(5);
                mockEngineVo.setRunningNum(5);
                when(workerService.calculationEngine(mockTaskDto,user,null)).thenReturn(mockEngineVo);
                Query mockQuery = new Query(Criteria.where("_id").is(taskId));
                mockQuery.fields().include("planStartDateFlag", "crontabExpressionFlag");
                when(taskRepository.findOne(mockQuery)).thenReturn(Optional.of(mockTask));
                boolean result = taskService.checkCloudTaskLimit(taskId,user,true);
                assertTrue(result);
            }
        }
    }

    @Nested
    class TestCopy{
        TaskRepository taskRepository = mock(TaskRepository.class);

        SettingsService settingsService = mock(SettingsService.class);

        TaskScheduleService taskScheduleService = mock(TaskScheduleService.class);

        UserLogService serLogService = mock(UserLogService.class);

        WorkerService workerService = mock(WorkerService.class);

        final UserDetail user = new UserDetail("6393f084c162f518b18165c3", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        @BeforeEach
        void beforeEach() {
            taskService = new TaskService(taskRepository);
            taskService.setSettingsService(settingsService);
            taskService.setTaskScheduleService(taskScheduleService);
            taskService.setUserLogService(serLogService);
            ReflectionTestUtils.setField(taskService,"workerService",workerService);
        }

        @Test
        void test_copySchedulingTask(){
            ObjectId taskId = MongoUtils.toObjectId("632454d5287a904778c40f8d");
            TaskEntity mockTask = new TaskEntity();
            mockTask.setCrontabExpressionFlag(true);
            mockTask.setCrontabExpression("test");
            mockTask.setId(MongoUtils.toObjectId("632454d5287a904778c40f8d"));
            try (MockedStatic<DataPermissionService> serviceMockedStatic = Mockito.mockStatic(DataPermissionService.class);
                 MockedStatic<SpringContextHelper> helperMockedStatic = Mockito.mockStatic(SpringContextHelper.class)) {
                serviceMockedStatic.when(DataPermissionService::isCloud).thenReturn(true);
                TaskDto mockTaskDto = new TaskDto();
                BeanUtils.copyProperties(mockTask,mockTaskDto);
                when(taskRepository.findById(taskId,user)).thenReturn(Optional.of(mockTask));
                when(settingsService.isCloud()).thenReturn(true);
                Query query = new Query(Criteria.where("_id").is(taskId));
                query.fields().include("id","agentId","agentTags");
                when(taskRepository.findOne(query)).thenReturn(Optional.of(mockTask));
                CalculationEngineVo mockEngineVo = new CalculationEngineVo();
                mockEngineVo.setTaskLimit(5);
                mockEngineVo.setRunningNum(4);
                when(workerService.calculationEngine(mockTaskDto,user,null)).thenReturn(mockEngineVo);
                Query mockQuery = new Query(Criteria.where("_id").is(taskId));
                mockQuery.fields().include("planStartDateFlag", "crontabExpressionFlag");
                when(taskRepository.findOne(mockQuery)).thenReturn(Optional.of(mockTask));
                TaskService mockTaskService = mock(TaskService.class);
                helperMockedStatic.when(()->SpringContextHelper.getBean(TaskService.class)).thenReturn(mockTaskService);
                when(mockTaskService.confirmById(any(TaskDto.class),any(UserDetail.class),any(Boolean.class))).thenAnswer(invocationOnMock -> {
                    return invocationOnMock.<TaskDto>getArgument(0);
                });
                TaskDto result = taskService.copy(taskId,user);
                assertTrue(result.getCrontabExpressionFlag());
                assertEquals("test",result.getCrontabExpression());
            }
        }

        @Test
        void test_copySchedulingTaskLimit(){
            ObjectId taskId = MongoUtils.toObjectId("632454d5287a904778c40f8d");
            TaskEntity mockTask = new TaskEntity();
            mockTask.setCrontabExpressionFlag(true);
            mockTask.setId(MongoUtils.toObjectId("632454d5287a904778c40f8d"));
            try (MockedStatic<DataPermissionService> serviceMockedStatic = Mockito.mockStatic(DataPermissionService.class);
                 MockedStatic<SpringContextHelper> helperMockedStatic = Mockito.mockStatic(SpringContextHelper.class)) {
                serviceMockedStatic.when(DataPermissionService::isCloud).thenReturn(true);
                TaskDto mockTaskDto = new TaskDto();
                BeanUtils.copyProperties(mockTask,mockTaskDto);
                when(taskRepository.findById(taskId,user)).thenReturn(Optional.of(mockTask));
                when(settingsService.isCloud()).thenReturn(true);
                Query query = new Query(Criteria.where("_id").is(taskId));
                query.fields().include("id","agentId","agentTags");
                when(taskRepository.findOne(query)).thenReturn(Optional.of(mockTask));
                CalculationEngineVo mockEngineVo = new CalculationEngineVo();
                mockEngineVo.setTaskLimit(5);
                mockEngineVo.setRunningNum(5);
                when(workerService.calculationEngine(mockTaskDto,user,null)).thenReturn(mockEngineVo);
                Query mockQuery = new Query(Criteria.where("_id").is(taskId));
                mockQuery.fields().include("planStartDateFlag", "crontabExpressionFlag");
                when(taskRepository.findOne(mockQuery)).thenReturn(Optional.of(mockTask));
                TaskService mockTaskService = mock(TaskService.class);
                helperMockedStatic.when(()->SpringContextHelper.getBean(TaskService.class)).thenReturn(mockTaskService);
                when(mockTaskService.confirmById(any(TaskDto.class),any(UserDetail.class),any(Boolean.class))).thenAnswer(invocationOnMock -> {
                    return invocationOnMock.<TaskDto>getArgument(0);
                });
                TaskDto result = taskService.copy(taskId,user);
                assertFalse(result.getCrontabExpressionFlag());
                assertNull(result.getCrontabExpression());
            }
        }

        @Test
        void test_copyNormalTaskLimit(){
            ObjectId taskId = MongoUtils.toObjectId("632454d5287a904778c40f8d");
            TaskEntity mockTask = new TaskEntity();
            mockTask.setId(MongoUtils.toObjectId("632454d5287a904778c40f8d"));
            try (MockedStatic<DataPermissionService> serviceMockedStatic = Mockito.mockStatic(DataPermissionService.class);
                 MockedStatic<SpringContextHelper> helperMockedStatic = Mockito.mockStatic(SpringContextHelper.class)) {
                serviceMockedStatic.when(DataPermissionService::isCloud).thenReturn(true);
                TaskDto mockTaskDto = new TaskDto();
                BeanUtils.copyProperties(mockTask,mockTaskDto);
                when(taskRepository.findById(taskId,user)).thenReturn(Optional.of(mockTask));
                when(settingsService.isCloud()).thenReturn(true);
                Query query = new Query(Criteria.where("_id").is(taskId));
                query.fields().include("id","agentId","agentTags");
                when(taskRepository.findOne(query)).thenReturn(Optional.of(mockTask));
                CalculationEngineVo mockEngineVo = new CalculationEngineVo();
                mockEngineVo.setTaskLimit(5);
                mockEngineVo.setRunningNum(4);
                when(workerService.calculationEngine(mockTaskDto,user,null)).thenReturn(mockEngineVo);
                Query mockQuery = new Query(Criteria.where("_id").is(taskId));
                mockQuery.fields().include("planStartDateFlag", "crontabExpressionFlag");
                when(taskRepository.findOne(mockQuery)).thenReturn(Optional.of(mockTask));
                TaskService mockTaskService = mock(TaskService.class);
                helperMockedStatic.when(()->SpringContextHelper.getBean(TaskService.class)).thenReturn(mockTaskService);
                when(mockTaskService.confirmById(any(TaskDto.class),any(UserDetail.class),any(Boolean.class))).thenAnswer(invocationOnMock -> {
                    return invocationOnMock.<TaskDto>getArgument(0);
                });
                TaskDto result = taskService.copy(taskId,user);
                assertNull(result.getCrontabExpressionFlag());
                assertNull(result.getCrontabExpression());
            }
        }
    }
    @Nested
    class TestRunningTaskNum{
        TaskRepository taskRepository = mock(TaskRepository.class);
        final UserDetail user = new UserDetail("6393f084c162f518b18165c3", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        @BeforeEach
        void beforeEach() {
            taskService = new TaskService(taskRepository);
        }
        @Test
        void testRunningTaskNum(){
            long except = 5L;
            when(taskRepository.count(Query.query(Criteria.where("is_deleted").ne(true)
                    .and("syncType").in(TaskDto.SYNC_TYPE_SYNC, TaskDto.SYNC_TYPE_MIGRATE)
                    .and("status").nin(TaskDto.STATUS_DELETE_FAILED,TaskDto.STATUS_DELETING)
                    .orOperator(Criteria.where("status").in(TaskDto.STATUS_RUNNING, TaskDto.STATUS_SCHEDULING, TaskDto.STATUS_WAIT_RUN),
                            Criteria.where("planStartDateFlag").is(true),
                            Criteria.where("crontabExpressionFlag").is(true)
                    )),user)).thenReturn(except);
            long result = taskService.runningTaskNum(user);
            assertEquals(except,result);
        }
    }
    @Nested
    class ChartTest{
        TaskRepository taskRepository = mock(TaskRepository.class);
        @Test
        void testChartNormal(){
            try (MockedStatic<DataPermissionService> mb = Mockito
                    .mockStatic(DataPermissionService.class)) {
                mb.when(DataPermissionService::isCloud).thenReturn(true);
                taskService = spy(new TaskService(taskRepository));
                UserDetail user = mock(UserDetail.class);
                DataPermissionMenuEnums permission = mock(DataPermissionMenuEnums.class);
                List<TaskDto> taskDtoList = new ArrayList<>();
                TaskDto taskDto1 = new TaskDto();
                taskDto1.setStatus("stop");
                taskDto1.setSyncType("migrate");
                TaskDto taskDto2 = new TaskDto();
                taskDto2.setStatus("wait_start");
                taskDto2.setSyncType("migrate");
                TaskDto taskDto3 = new TaskDto();
                taskDto3.setStatus("edit");
                taskDto3.setSyncType("migrate");
                TaskDto taskDto4 = new TaskDto();
                taskDto4.setStatus("stop");
                taskDto4.setSyncType("sync");
                TaskDto taskDto5 = new TaskDto();
                taskDto5.setStatus("stop");
                taskDto5.setSyncType("sync");
                taskDtoList.add(taskDto1);
                taskDtoList.add(taskDto2);
                taskDtoList.add(taskDto3);
                taskDtoList.add(taskDto4);
                taskDtoList.add(taskDto5);
                doReturn(taskDtoList).when(taskService).findAllDto(any(),any());
                when(permission.MigrateTack.checkAndSetFilter(user, DataPermissionActionEnums.View, () -> taskService.findAllDto(any(),any()))).thenReturn(taskDtoList);
                doReturn(new HashMap()).when(taskService).inspectChart(user);
                Chart6Vo chart6Vo = mock(Chart6Vo.class);
                doReturn(chart6Vo).when(taskService).chart6(user);
                Map<String, Object> actual = taskService.chart(user);
                Map chart1 = (Map) actual.get("chart1");
                assertEquals(3,chart1.get("total"));
                Map chart3 = (Map) actual.get("chart3");
                assertEquals(2,chart3.get("total"));
                Map chart5 = (Map) actual.get("chart5");
                assertEquals(0,chart5.size());
                assertEquals(chart6Vo,actual.get("chart6"));
            }

        }
    }
    @Nested
    class importRmProjectTest{
        TaskRepository taskRepository=mock(TaskRepository.class);
        TaskService taskService=spy(new TaskService(taskRepository));
        UserDetail userDetail;
        FileInputStream fileInputStream;
        @BeforeEach
        void beforeEach() throws FileNotFoundException {
            userDetail = new UserDetail("6393f084c162f518b18165c3", "customerId", "username", "password", "customerType",
                    "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
            URL resource = this.getClass().getClassLoader().getResource("test.relmig");
            fileInputStream=new FileInputStream(resource.getFile());
        }
        @Test
        void importRmProjectTest() throws IOException {
            MockMultipartFile mockMultipartFile = new MockMultipartFile("test.relmig", fileInputStream);
            String rmJson = new String(mockMultipartFile.getBytes());
            HashMap<String, Object> rmProject = new ObjectMapper().readValue(rmJson, HashMap.class);
            HashMap<String, Object> project = (HashMap<String, Object>) rmProject.get("project");
            HashMap<String, Object> content = (HashMap<String, Object>) project.get("content");
            HashMap<String, Object> contentCollections = (HashMap<String, Object>) content.get("collections");
            Map<String, String> stringStringMap = taskService.parseTaskFromRm(rmJson, "123", "123", userDetail);
            TaskDto taskDto=null;
            for(String taskKey:stringStringMap.keySet()){
                taskDto = JsonUtil.parseJsonUseJackson(stringStringMap.get(taskKey), TaskDto.class);
            }
            for(String key:contentCollections.keySet()){
                Map<String,String> contentCollectionMap = (Map<String, String>) contentCollections.get(key);
                String afterName = contentCollectionMap.get("name");
                assertEquals(taskDto.getName(),afterName);
            }
            assertEquals("6393f084c162f518b18165c3",taskDto.getUserId());
            assertEquals("customerId",taskDto.getCustomId());
            assertEquals("customer5",taskDto.getDag().getNodes().get(0).getName());
            assertEquals("customer5",taskDto.getDag().getNodes().get(2).getName());
            assertEquals("customer5",taskDto.getDag().getNodes().get(1).getName());
        }
        @Test
        void nullImportRmProjectTest(){
            assertThrows(BizException.class,()->{taskService.parseTaskFromRm(null, "123", "123", userDetail);});
        }
        @Test
        void replaceIdTest() throws IOException {
            MockMultipartFile mockMultipartFile = new MockMultipartFile("test.relmig", fileInputStream);
            String s = new String(mockMultipartFile.getBytes());
            Map<String, Object> rmProject = new ObjectMapper().readValue(s, HashMap.class);
            Map<String, Object> project = (Map<String, Object>) rmProject.get("project");
            Map<String, Object> content = (Map<String, Object>) project.get("content");
            Map<String, Object> contentMapping = (Map<String, Object>) content.get("mappings");
            Map<String, Object> contentCollections = (Map<String, Object>) content.get("collections");
            Set<String> collectionKeys = contentCollections.keySet();
            String collectionKey=null;
            for(String key:collectionKeys){
                collectionKey=key;
            }
            Set<String> contentMappingKeys = contentMapping.keySet();
            String contentMappingKey=null;
            String contentMappingCollectionId=null;
            for(String key:contentMappingKeys){
                Map<String, Object> mapping = (Map<String, Object>) contentMapping.get(key);
                String collectionId = (String)mapping.get("collectionId");
                contentMappingCollectionId=collectionId;
                contentMappingKey=key;
            }
            taskService.replaceRmProjectId(rmProject);
            Set<String> afterStrings = contentCollections.keySet();
            String afterCollectionKey=null;
            for(String afterKey1:afterStrings){
                afterCollectionKey=afterKey1;
            }
            Set<String> afterContentMappingKeys = contentMapping.keySet();
            String afterContentMappingCollectionId=null;
            String afterContentMappingKey=null;
            for(String key:afterContentMappingKeys){
                Map<String, Object> mapping = (Map<String, Object>) contentMapping.get(key);
                afterContentMappingCollectionId = (String)mapping.get("collectionId");
                afterContentMappingKey=key;
            }
            assertNotEquals(collectionKey,afterCollectionKey);
            assertNotEquals(contentMappingKey,afterContentMappingKey);
            assertNotEquals(contentMappingCollectionId,afterContentMappingCollectionId);
        }
        @Test
        void testReplaceRelationShipsKey() throws IOException {
            Map<String, String> globalIdMap = new HashMap<>();
            MockMultipartFile mockMultipartFile = new MockMultipartFile("test.relmig", fileInputStream);
            String s = new String(mockMultipartFile.getBytes());
            Map<String, Object> rmProject = new ObjectMapper().readValue(s, HashMap.class);
            Map<String, Object> project = (Map<String, Object>) rmProject.get("project");
            Map<String, Object> content = (Map<String, Object>) project.get("content");
            Map<String, Object> relationships = content.get("relationships") == null ? new HashMap<>() : (Map<String, Object>) content.get("relationships");
            Map<String, Object> collectionMap = (Map<String, Object>) relationships.get("collections");
            Map<String, Object> mappingsMap = (Map<String, Object>) relationships.get("mappings");
            String collectionKey=null;
            for(String key:collectionMap.keySet()){
                collectionKey=key;
            }
            String mappingKey=null;
            for(String key:mappingsMap.keySet()){
                mappingKey=key;
            }
            String relationShipMappingsKey=null;
            for(String key:mappingsMap.keySet()){
                relationShipMappingsKey=key;
            }
            taskService.replaceRelationShipsKey(globalIdMap,content);
            String afterCollectionKey=null;
            for(String key:collectionMap.keySet()){
                afterCollectionKey=key;
            }
            String afterMappingKey=null;
            for(String key:mappingsMap.keySet()){
                afterMappingKey=key;
            }
            String afterRelationShipMappingsKey=null;
            for(String key:mappingsMap.keySet()){
                afterRelationShipMappingsKey=key;
            }
            assertNotEquals(collectionKey,afterCollectionKey);
            assertNotEquals(afterMappingKey,mappingKey);
            assertNotEquals(afterRelationShipMappingsKey,relationShipMappingsKey);
        }
        @Test
        void testImportRmProject() throws IOException {
            CustomSqlService customSqlService = mock(CustomSqlService.class);
            taskService.setCustomSqlService(customSqlService);
            DateNodeService dataNodeService = mock(DateNodeService.class);
            taskService.setDateNodeService(dataNodeService);
            MockMultipartFile mockMultipartFile = new MockMultipartFile("test.relmig", fileInputStream);
            String s = new String(mockMultipartFile.getBytes());
            Map<String, String> stringStringMap = taskService.parseTaskFromRm(s, "123", "123", userDetail);
            TaskDto taskDto=null;
            for(String s1: stringStringMap.keySet()){
                taskDto = JsonUtil.parseJsonUseJackson(stringStringMap.get(s1), TaskDto.class);
            }
            try (MockedStatic<BeanUtils> beanUtilsMockedStatic = mockStatic(BeanUtils.class);MockedStatic<DataPermissionHelper> dataPermissionHelperMockedStatic = mockStatic(DataPermissionHelper.class)) {
                BeanUtils.copyProperties(any(),any());
                TaskEntity taskEntity = taskService.convertToEntity(TaskEntity.class, taskDto);
                when(taskRepository.importEntity(any(),any())).thenReturn(taskEntity);
                MongoTemplate mongoTemplate = mock(MongoTemplate.class);
                when(taskRepository.getMongoOperations()).thenReturn(mongoTemplate);
                assertThrows(BizException.class,()->{taskService.importRmProject(mockMultipartFile,userDetail,false,new ArrayList<>(),"123","123");});
            }
        }
        @Test
        void testCheckJsProcessorTestRun() throws IOException {
            MockMultipartFile mockMultipartFile = new MockMultipartFile("test.relmig", fileInputStream);
            String s = new String(mockMultipartFile.getBytes());
            Map<String, String> stringStringMap = taskService.parseTaskFromRm(s, "123", "123", userDetail);
            TaskDto taskDto=null;
            for(String s1: stringStringMap.keySet()){
                taskDto = JsonUtil.parseJsonUseJackson(stringStringMap.get(s1), TaskDto.class);
                taskDto.setId(new ObjectId("659d296d5ae70acad4e3f0db"));
            }
            List<TaskDto> taskDtos = Arrays.asList(taskDto);
            TaskNodeService taskNodeService = mock(TaskNodeService.class);
            taskService.setTaskNodeService(taskNodeService);
            doAnswer(invocationOnMock -> {
                TestRunDto testRunDto = (TestRunDto) invocationOnMock.getArgument(0);
                assertEquals(1,testRunDto.getRows());
                assertEquals("659d296d5ae70acad4e3f0db",testRunDto.getTaskId());
                return null;
            }).when(taskNodeService).testRunJsNodeRPC(any(TestRunDto.class),any(UserDetail.class),anyInt());
            taskService.checkJsProcessorTestRun(userDetail,taskDtos);
        }
        @Test
        void testGenProperties() throws IOException {
            URL resource = this.getClass().getClassLoader().getResource("EmployeeSchema.relmig");
            FileInputStream fileInputStream = new FileInputStream(resource.getFile());
            MockMultipartFile mockMultipartFile = new MockMultipartFile("EmployeeSchema.relmig", fileInputStream);
            String s = new String(mockMultipartFile.getBytes());
            Map<String, String> stringStringMap = taskService.parseTaskFromRm(s, "123", "123", userDetail);
            TaskDto taskDto = null;
            for (String key : stringStringMap.keySet()) {
                System.out.println();
                taskDto = JsonUtil.parseJsonUseJackson(stringStringMap.get(key), TaskDto.class);
            }
            ;
            List<Node> nodes = taskDto.getDag().getNodes();
            boolean flag = false;
            for (Node node : nodes) {
                if (node.getType().equals("merge_table_processor")) {
                    flag = true;
                }
            }
            assertTrue(flag);
        }
    }
    @Nested
    class ParentColumnsFindJoinKeysClass{
        TaskRepository taskRepository=mock(TaskRepository.class);
        TaskService taskService=spy(new TaskService(taskRepository));
        Map<String, Object> parent;
        Map<String, Map<String, Map<String, Object>>> renameFields;


        @BeforeEach
        void beforeSetUp(){
            parent = new HashMap<>();
            parent.put("rm_id", "rm_id -> eb1243b6-e7dc-4b84-b094-e719f9275512");
            parent.put("tableName", "Orders");
            renameFields = new HashMap<>();

            Map<String,Map<String,Object>> orderFieldMap=new HashMap<>();
            Map<String,Object> idAttrs=new HashMap<>();
            idAttrs.put("isPrimaryKey", false);
            idAttrs.put("target","_id");
            Map<String,Object> orderIdAttrs=new HashMap<>();
            orderIdAttrs.put("isPrimaryKey",true);
            orderIdAttrs.put("target","orderId");
            Map<String,Object> shipViaAttrs=new HashMap<>();
            shipViaAttrs.put("target","shipVia");
            shipViaAttrs.put("isPrimaryKey",false);
            orderFieldMap.put("ShipVia",shipViaAttrs);
            orderFieldMap.put("_id",idAttrs);
            orderFieldMap.put("OrderID",orderIdAttrs);

            Map<String,Map<String,Object>> shipperFieldMap=new HashMap<>();
            Map<String,Object> shipperIdAttrs=new HashMap<>();
            shipperIdAttrs.put("target","shipperId");
            shipperIdAttrs.put("isPrimaryKey",true);
            shipperFieldMap.put("ShipperID",shipperIdAttrs);

            renameFields.put("Shippers",shipperFieldMap);
            renameFields.put("Orders",orderFieldMap);
        }
        @DisplayName("test parent column have foreignKey,and foreignKey table is child table")
        @Test
        void test1(){
            parent.put("targetPath", "");
            List<Map<String, String>> joinKeys=new ArrayList<>();
            Map<String, Object> parentColumns=new HashMap<>();
            Map<String, Object> columnsAttrs=new HashMap<>();
            Map<String,Object> foreignKeyAttrs=new HashMap<>();
            foreignKeyAttrs.put("name","FK_Orders_Shippers");
            foreignKeyAttrs.put("table","Shippers");
            foreignKeyAttrs.put("column","ShipperID");
            columnsAttrs.put("foreignKey",foreignKeyAttrs);
            parentColumns.put("ShipVia",columnsAttrs);
            taskService.parentColumnsFindJoinKeys(parent,renameFields,parentColumns,"Shippers",joinKeys);
            assertEquals(1,joinKeys.size());
            Map<String, String> stringStringMap = joinKeys.get(0);
            String sourceJoinKey = stringStringMap.get("source");
            String targetJoinKey = stringStringMap.get("target");
            assertEquals("shipperId",sourceJoinKey);
            assertEquals("shipVia",targetJoinKey);
        }
        @DisplayName("test parent column have foreignKey,foreignKey table is child table and have targetPath")
        @Test
        void test2(){
            parent.put("targetPath", "orders");
            List<Map<String, String>> joinKeys=new ArrayList<>();
            Map<String, Object> parentColumns=new HashMap<>();
            Map<String, Object> columnsAttrs=new HashMap<>();
            Map<String,Object> foreignKeyAttrs=new HashMap<>();
            foreignKeyAttrs.put("name","FK_Orders_Shippers");
            foreignKeyAttrs.put("table","Shippers");
            foreignKeyAttrs.put("column","ShipperID");
            columnsAttrs.put("foreignKey",foreignKeyAttrs);
            parentColumns.put("ShipVia",columnsAttrs);
            taskService.parentColumnsFindJoinKeys(parent,renameFields,parentColumns,"Shippers",joinKeys);
            assertEquals(1,joinKeys.size());
            Map<String, String> stringStringMap = joinKeys.get(0);
            String sourceJoinKey = stringStringMap.get("source");
            String targetJoinKey = stringStringMap.get("target");
            assertEquals("shipperId",sourceJoinKey);
            assertEquals("orders.shipVia",targetJoinKey);
        }
        @DisplayName("test parent column no have foreignKey")
        @Test
        void test3(){
            List<Map<String, String>> joinKeys=new ArrayList<>();
            Map<String, Object> parentColumns=new HashMap<>();
            Map<String, Object> columnsAttrs=new HashMap<>();
            parentColumns.put("ShipVia",columnsAttrs);
            taskService.parentColumnsFindJoinKeys(parent,renameFields,parentColumns,"Shippers",joinKeys);
            assertEquals(0,joinKeys.size());
        }
        @DisplayName("test parnet column table is not child table")
        @Test
        void test4(){
            parent.put("targetPath", "");
            List<Map<String, String>> joinKeys=new ArrayList<>();
            Map<String, Object> parentColumns=new HashMap<>();
            Map<String, Object> columnsAttrs=new HashMap<>();
            Map<String,Object> foreignKeyAttrs=new HashMap<>();
            foreignKeyAttrs.put("name","FK_Orders_Shippers");
            foreignKeyAttrs.put("table","testTable");
            foreignKeyAttrs.put("column","ShipperID");
            columnsAttrs.put("foreignKey",foreignKeyAttrs);
            parentColumns.put("ShipVia",columnsAttrs);
            taskService.parentColumnsFindJoinKeys(parent,renameFields,parentColumns,"Shippers",joinKeys);
            assertEquals(0,joinKeys.size());
        }
    }
    @Nested
    class RunningTaskNumWithProcessIdTest{
        @Test
        void testRunningTaskNumWithProcessId(){
            TaskRepository taskRepository = mock(TaskRepository.class);
            taskService = new TaskService(taskRepository);
            long except = 5L;
            UserDetail userDetail = mock(UserDetail.class);
            when(taskRepository.count(Query.query(Criteria.where("agentId").is("111")
                    .and("is_deleted").ne(true).and("syncType").in(TaskDto.SYNC_TYPE_SYNC, TaskDto.SYNC_TYPE_MIGRATE)
                    .and("status").nin(TaskDto.STATUS_DELETE_FAILED,TaskDto.STATUS_DELETING)
                    .orOperator(Criteria.where("status").in(TaskDto.STATUS_RUNNING, TaskDto.STATUS_SCHEDULING, TaskDto.STATUS_WAIT_RUN),
                            Criteria.where("planStartDateFlag").is(true),
                            Criteria.where("crontabExpressionFlag").is(true)
                    )), userDetail)).thenReturn(except);
            long result = taskService.runningTaskNum("111", userDetail);
            assertEquals(except,result);
        }
    }

}
