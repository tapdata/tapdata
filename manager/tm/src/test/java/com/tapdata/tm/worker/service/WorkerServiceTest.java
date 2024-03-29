package com.tapdata.tm.worker.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.constant.SettingUtil;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.reporitory.BaseRepository;
import com.tapdata.tm.cluster.service.ClusterStateService;
import com.tapdata.tm.commons.base.dto.SchedulableDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.SimpleGrantedAuthority;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.IDataPermissionHelper;
import com.tapdata.tm.permissions.service.DataPermissionService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.dto.WorkerProcessInfoDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.repository.WorkerRepository;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import org.bson.BsonValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class WorkerServiceTest {
    private WorkerService workerService;
    private WorkerRepository workerRepository;
    private SettingsService settingsService;

    private TaskService taskService;

    private ClusterStateService clusterStateService;
    @BeforeEach
    void buildWorkService(){
        new DataPermissionHelper(mock(IDataPermissionHelper.class)); //when repository.find call methods in DataPermissionHelper class this line is need
        workerRepository = mock(WorkerRepository.class);
        workerService = spy(new WorkerServiceImpl(workerRepository));
        settingsService = mock(SettingsService.class);
        taskService = mock(TaskService.class);
        clusterStateService = mock(ClusterStateService.class);
        ReflectionTestUtils.setField(workerService,"settingsService",settingsService);
        ReflectionTestUtils.setField(workerService,"taskService",taskService);
        ReflectionTestUtils.setField(workerService,"clusterStateService",clusterStateService);
    }
    @Test
    void testQueryWorkerByProcessIdWithoutId(){
        String processId = "  ";
        assertThrows(IllegalArgumentException.class,()->workerService.queryWorkerByProcessId(processId));
    }
    @Test
    void testQueryWorkerByProcessIdWithId(){
        String processId = "111";
        WorkerDto workerDto = mock(WorkerDto.class);
        Query query = Query.query(Criteria.where("process_id").is(processId).and("worker_type").is("connector"));
        doReturn(workerDto).when(workerService).findOne(query);
        WorkerDto actual = workerService.queryWorkerByProcessId(processId);
        assertEquals(workerDto,actual);
    }
    @Test
    void testQueryAllBindWorker(){
        List<Worker> excepted = new ArrayList<>();
        Query query = Query.query(Criteria.where("worker_type").is("connector").and("licenseBind").is(true));
        BaseRepository repository = mock(BaseRepository.class);
        when(repository.findAll(query)).thenReturn(excepted);
        List<Worker> actual = workerService.queryAllBindWorker();
        assertEquals(excepted,actual);
    }
    @Test
    void testBindByProcessIdWithoutId(){
        WorkerDto workerDto = mock(WorkerDto.class);
        UserDetail userDetail = mock(UserDetail.class);
        String processId = "  ";
        assertThrows(IllegalArgumentException.class,()->workerService.bindByProcessId(workerDto,processId,userDetail));
    }
    @Test
    void testBindByProcessIdWithId(){
        String processId = "111";
        WorkerDto workerDto = mock(WorkerDto.class);
        UserDetail userDetail = mock(UserDetail.class);
        Query query = Query.query(Criteria.where("process_id").is(processId).and("worker_type").is("connector"));
        Update update = Update.update("licenseBind", true);
        UpdateResult result = new UpdateResult() {
            @Override
            public boolean wasAcknowledged() {
                return false;
            }
            @Override
            public long getMatchedCount() {
                return 1;
            }
            @Override
            public long getModifiedCount() {
                return 1;
            }
            @Override
            public BsonValue getUpsertedId() {
                return null;
            }
        };
        doReturn(workerDto).when(workerService).queryWorkerByProcessId(processId);
        doReturn(result).when(workerRepository).update(query,update);
        boolean actual = workerService.bindByProcessId(workerDto, processId, userDetail);
        assertEquals(true,actual);
    }
    @Test
    void testBindByProcessIdWithNullResult(){
        String processId = "111";
        WorkerDto workerDto = mock(WorkerDto.class);
        UserDetail userDetail = mock(UserDetail.class);
        Query query = Query.query(Criteria.where("process_id").is(processId).and("worker_type").is("connector"));
        Update update = Update.update("licenseBind", true);
        doReturn(workerDto).when(workerService).queryWorkerByProcessId(processId);
        doReturn(null).when(workerRepository).update(query,update);
        boolean actual = workerService.bindByProcessId(workerDto, processId, userDetail);
        assertEquals(false,actual);
    }
    @Test
    void testBindByProcessIdWithCount(){
        String processId = "111";
        WorkerDto workerDto = mock(WorkerDto.class);
        UserDetail userDetail = mock(UserDetail.class);
        Query query = Query.query(Criteria.where("process_id").is(processId).and("worker_type").is("connector"));
        Update update = Update.update("licenseBind", true);
        UpdateResult result = new UpdateResult() {
            @Override
            public boolean wasAcknowledged() {
                return false;
            }
            @Override
            public long getMatchedCount() {
                return 1;
            }
            @Override
            public long getModifiedCount() {
                return 0;
            }
            @Override
            public BsonValue getUpsertedId() {
                return null;
            }
        };
        doReturn(workerDto).when(workerService).queryWorkerByProcessId(processId);
        doReturn(result).when(workerRepository).update(query,update);
        boolean actual = workerService.bindByProcessId(workerDto, processId, userDetail);
        assertEquals(false,actual);
    }
    @Test
    void testBindByProcessIdWithIdSave(){
        String processId = "111";
        WorkerDto workerDto = mock(WorkerDto.class);
        UserDetail userDetail = mock(UserDetail.class);
        Query query = Query.query(Criteria.where("process_id").is(processId).and("worker_type").is("connector"));
        Update update = Update.update("licenseBind", true);
        UpdateResult result = new UpdateResult() {
            @Override
            public boolean wasAcknowledged() {
                return false;
            }
            @Override
            public long getMatchedCount() {
                return 1;
            }
            @Override
            public long getModifiedCount() {
                return 1;
            }
            @Override
            public BsonValue getUpsertedId() {
                return null;
            }
        };
        doReturn(null).when(workerService).queryWorkerByProcessId(processId);
        doReturn(workerDto).when(workerService).save(workerDto,userDetail);
        doReturn(result).when(workerRepository).update(query,update);
        boolean actual = workerService.bindByProcessId(workerDto, processId, userDetail);
        assertEquals(true,actual);
    }
    @Test
    void testUnbindByProcessIdWithoutId(){
        String processId = "  ";
        assertThrows(IllegalArgumentException.class,()->workerService.unbindByProcessId(processId));
    }
    @Test
    void testUnbindByProcessIdWithId(){
        String processId = "111";
        Query query = Query.query(Criteria.where("process_id").is(processId).and("worker_type").is("connector"));
        Update update = Update.update("licenseBind", false);
        UpdateResult result = new UpdateResult() {
            @Override
            public boolean wasAcknowledged() {
                return false;
            }
            @Override
            public long getMatchedCount() {
                return 1;
            }
            @Override
            public long getModifiedCount() {
                return 1;
            }
            @Override
            public BsonValue getUpsertedId() {
                return null;
            }
        };
        when(workerRepository.update(query,update)).thenReturn(result);
        boolean actual = workerService.unbindByProcessId(processId);
        assertEquals(true,actual);
    }

    @Test
    void testCalculationEngine(){
        SchedulableDto mockSchedulable = new SchedulableDto();
        UserDetail user = new UserDetail("6393f084c162f518b18165c3", "customerId", "username", "password", "customerType",
                "accessCode", false, false, false, false, Arrays.asList(new SimpleGrantedAuthority("role")));
        try (MockedStatic<DataPermissionService> serviceMockedStatic = Mockito.mockStatic(DataPermissionService.class)){
            when(settingsService.isCloud()).thenReturn(false);
            Settings settings = new Settings();
            settings.setValue("1000");
            when(settingsService.getByCategoryAndKey(CategoryEnum.WORKER, KeyEnum.WORKER_HEART_TIMEOUT)).thenReturn(settings);
            serviceMockedStatic.when(DataPermissionService::isCloud).thenReturn(true);
            Worker worker = new Worker();
            worker.setUserId("6393f084c162f518b18165c3");
            worker.setProcessId("test");
            List<Worker> workers = Arrays.asList(worker);
            int expected = 5;
            when(workerRepository.findAll(any(Query.class))).thenReturn(workers);
            when(taskService.runningTaskNum("test",user)).thenReturn(expected);
            when(taskService.runningTaskNum(user)).thenReturn(expected);
            CalculationEngineVo calculationEngineVo = workerService.calculationEngine(mockSchedulable,user,null);
            assertEquals(expected,calculationEngineVo.getRunningNum());
        }
    }

    @Test
    void test_getLastCheckAvailableAgentCount(){
        try(MockedStatic<SettingUtil> mockedStatic = mockStatic(SettingUtil.class)){
            mockedStatic.when(()->SettingUtil.getValue(anyString(),anyString())).thenReturn("300");
            Long except = 100L;
            when(workerRepository.count(any(Query.class))).thenReturn(except);
            Long result = workerService.getLastCheckAvailableAgentCount();
            assertEquals(except,result);
        }
    }
    @Test
    void test_getProcessInfo(){
        try (MockedStatic<DataPermissionService> serviceMockedStatic = Mockito.mockStatic(DataPermissionService.class)){
            serviceMockedStatic.when(DataPermissionService::isCloud).thenReturn(true);
            List<Worker> mockWorkers = new ArrayList<>();
            Worker mockWorker = new Worker();
            mockWorker.setProcessId("test");
            mockWorkers.add(mockWorker);
            when(workerRepository.findAll(Query.query(Criteria.where("workerType").is("connector").and("process_id").in("test")))).thenReturn(mockWorkers);
            List<TaskDto> mockTaskList = new ArrayList<>();
            TaskDto taskDto = new TaskDto();
            taskDto.setSyncType("sync");
            taskDto.setId(MongoUtils.toObjectId("64ce7d3794076a1af015e50c"));
            taskDto.setName("test");
            mockTaskList.add(taskDto);
            when(taskService.findAll(any(Query.class))).thenReturn(mockTaskList);
            Map<String, WorkerProcessInfoDto> result =  workerService.getProcessInfo(Arrays.asList("test"),mock(UserDetail.class));
            assertEquals(1,result.get("test").getRunningNum());
        }
    }
}
