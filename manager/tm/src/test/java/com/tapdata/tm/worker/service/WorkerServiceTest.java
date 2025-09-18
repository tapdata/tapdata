package com.tapdata.tm.worker.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.constant.SettingUtil;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.exception.BizException;
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
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.HttpUtils;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.dto.WorkerExpireDto;
import com.tapdata.tm.worker.dto.WorkerProcessInfoDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.entity.WorkerExpire;
import com.tapdata.tm.worker.repository.WorkerRepository;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import org.bson.BsonValue;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

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
    @Nested
    class GetWorkerCurrentTimeTest{
        @DisplayName("Test main process")
        @Test
        void test() throws ParseException {
            String date = "2024-04-26 11:00:00";
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            List<Worker> workerList = new ArrayList<>();
            Worker worker = new Worker();
            worker.setWorkerDate(simpleDateFormat.parse(date));
            workerList.add(worker);
            doReturn(workerList).when(workerService).findAvailableAgent(any());
            String result = workerService.getWorkerCurrentTime(mock(UserDetail.class));
            Assertions.assertEquals(date,result);
        }

        @DisplayName("workerDate is null")
        @Test
        void test1(){
            List<Worker> workerList = new ArrayList<>();
            Worker worker = new Worker();
            workerList.add(worker);
            doReturn(workerList).when(workerService).findAvailableAgent(any());
            String result = workerService.getWorkerCurrentTime(mock(UserDetail.class));
            Assertions.assertNotNull(result);
        }

    }
    @Nested
    class checkEngineVersionTest{
        @DisplayName("Test main process")
        @Test
        void test(){
            when(settingsService.isCloud()).thenReturn(true);
            List<Worker> workers = new ArrayList<>();
            Worker worker1 = new Worker();
            worker1.setVersion("v3.7.0-65db776b");
            Worker worker2 = new Worker();
            worker2.setVersion("v3.5.11-65db776b");
            workers.add(worker1);
            workers.add(worker2);
            doReturn(workers).when(workerService).findAvailableAgent(any());
            Assertions.assertFalse(workerService.checkEngineVersion(mock(UserDetail.class)));
        }

        @DisplayName("Test version matching")
        @Test
        void testEngineVersionIsCloud(){
            when(settingsService.isCloud()).thenReturn(true);
            List<Worker> workers = new ArrayList<>();
            Worker worker1 = new Worker();
            worker1.setVersion("v3.8.0-65db776b");
            workers.add(worker1);
            doReturn(workers).when(workerService).findAvailableAgent(any());
            Assertions.assertTrue(workerService.checkEngineVersion(mock(UserDetail.class)));
        }

        @DisplayName("Test version mismatch")
        @Test
        void testEngineVersionMismatch(){
            when(settingsService.isCloud()).thenReturn(true);
            List<Worker> workers = new ArrayList<>();
            Worker worker1 = new Worker();
            worker1.setVersion("v3.8.0-65db776b");
            Worker worker2 = new Worker();
            worker2.setVersion("v3.5.11-65db776b");
            workers.add(worker1);
            workers.add(worker2);
            doReturn(workers).when(workerService).findAvailableAgent(any());
            Assertions.assertFalse(workerService.checkEngineVersion(mock(UserDetail.class)));
        }

        @DisplayName("Test version Worker is Null")
        @Test
        void testWorkerIsNull(){
            when(settingsService.isCloud()).thenReturn(true);
            List<Worker> workers = new ArrayList<>();
            doReturn(workers).when(workerService).findAvailableAgent(any());
            Assertions.assertFalse(workerService.checkEngineVersion(mock(UserDetail.class)));
        }

        @DisplayName("Test version Worker version is null")
        @Test
        void testWorkerVersionIsNull(){
            when(settingsService.isCloud()).thenReturn(true);
            List<Worker> workers = new ArrayList<>();
            Worker worker1 = new Worker();
            worker1.setVersion(null);
            workers.add(worker1);
            doReturn(workers).when(workerService).findAvailableAgent(any());
            Assertions.assertFalse(workerService.checkEngineVersion(mock(UserDetail.class)));
        }

        @DisplayName("Test version is DASS")
        @Test
        void testEngineVersionDASS(){
            when(settingsService.isCloud()).thenReturn(false);
            Assertions.assertTrue(workerService.checkEngineVersion(mock(UserDetail.class)));
        }
    }
    @Nested
    class createShareWorkerTest {
        private WorkerExpireDto workerExpireDto;
        private UserDetail loginUser;
        private UserService userService;
        private MongoTemplate mongoTemplate;
        @BeforeEach
        public void setUp() {
            workerExpireDto = new WorkerExpireDto();
            workerExpireDto.setUserId("testUser");
            workerExpireDto.setSubscribeId("subscribe123");

            loginUser = mock(UserDetail.class);
            loginUser.setUserId("loginUser");
            loginUser.setUsername("loginUsername");
            mongoTemplate = mock(MongoTemplate.class);
            userService = mock(UserService.class);
            ReflectionTestUtils.setField(workerService, "userService", userService);
            ReflectionTestUtils.setField(workerService, "mongoTemplate", mongoTemplate);
            doCallRealMethod().when(workerService).createShareWorker(workerExpireDto, loginUser);
        }

        @Test
        public void testCreateShareWorker_Success() {
            when(settingsService.getValueByCategoryAndKey(CategoryEnum.SYSTEM, KeyEnum.SHARE_AGENT_EXPRIRE_DAYS))
                    .thenReturn("30");
            when(settingsService.getValueByCategoryAndKey(CategoryEnum.SYSTEM, KeyEnum.SHARE_AGENT_CREATE_USER))
                    .thenReturn("agentUser1,agentUser2");

            UserDetail sharedUserDetail = mock(UserDetail.class);
            sharedUserDetail.setUserId("sharedUserId");
            sharedUserDetail.setTcmUserId("sharedTcmUserId");
            when(userService.loadUserByUsername(anyString()))
                    .thenReturn(sharedUserDetail);
            when(mongoTemplate.findOne(any(), eq(WorkerExpire.class)))
                    .thenReturn(null);
            workerService.createShareWorker(workerExpireDto, loginUser);
            verify(mongoTemplate, times(1)).insert(any(WorkerExpire.class));
            verify(userService, times(1)).loadUserByUsername(anyString());
        }

        @Test
        public void testCreateShareWorker_UserAlreadyExists() {
            WorkerExpire existingWorkerExpire = new WorkerExpire();
            when(mongoTemplate.findOne(any(), eq(WorkerExpire.class)))
                    .thenReturn(existingWorkerExpire);
            BizException exception = assertThrows(BizException.class, () -> {
                workerService.createShareWorker(workerExpireDto, loginUser);
            });

            assertEquals("SHARE_AGENT_USER_EXISTED", exception.getErrorCode());
            assertEquals("have applied for a public agent", exception.getMessage());
            verify(mongoTemplate, never()).insert(any(WorkerExpire.class));
        }

        @Test
        public void testCreateShareWorker_UserNotFound() {
            when(settingsService.getValueByCategoryAndKey(CategoryEnum.SYSTEM, KeyEnum.SHARE_AGENT_EXPRIRE_DAYS))
                    .thenReturn("30");
            when(settingsService.getValueByCategoryAndKey(CategoryEnum.SYSTEM, KeyEnum.SHARE_AGENT_CREATE_USER))
                    .thenReturn("agentUser1,agentUser2");
            when(userService.loadUserByUsername(anyString()))
                    .thenReturn(null);
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
                workerService.createShareWorker(workerExpireDto, loginUser);
            });

            assertEquals("userDetail is null", exception.getMessage());
            verify(mongoTemplate, never()).insert(any(WorkerExpire.class));
        }
    }

    @Test
    void test1() {
        String token =
"eyJraWQiOiIwYTJmMDgzZS1lMzZkLTQ0NjctYmFiYi1mMGM2MmVhN2M5ZjIiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiI1YzBlNzUwYjdhNWNkNDI0NjRhNTA5OWQiLCJhdWQiOiI1YzBlNzUwYjdhNWNkNDI0NjRhNTA5OWQiLCJjcmVhdGVkQXQiOjE3NTc2Njc1OTQwODksIm5iZiI6MTc1NzY2NzU5NCwiY2xpZW50SWQiOiI1YzBlNzUwYjdhNWNkNDI0NjRhNTA5OWQiLCJyb2xlcyI6WyIkZXZlcnlvbmUiLCJhZG1pbiJdLCJpc3MiOiJodHRwOi8vMTI3LjAuMC4xOjMwMDAiLCJleHBpcmVkYXRlIjoxNzU3NjY3ODk0MDg5LCJleHAiOjE3NTc2Njc4OTQsImlhdCI6MTc1NzY2NzU5NCwianRpIjoiNzY0ZGI1N2MtYmU3Ni00YmJmLTlmNDgtMmQ4N2ViMmQ4ZmFjIn0.IvVzcmDFWDJBwxgDBMTYJ-DEmNEHf2Gq77BRbd3zrfuTRiBUboKUhGMLqDVfC2E7i9WQdKaiaDa0uY2BS7huO586O3rt4dntlMcE334X1rMlIzf0i1DhJb-Ihz8b9E0_Dfqu93XVluO8L77KCaqyKLyIZTUzxK9YmFlCSbmeIRofEqOibLpFeRY6uxUOK5MwutVkqe_zOahgTdnklx7ClyzwFSsKrTpMPanJVjPS1oSWdP29t3zKhyoJkCpAhUPTkxu3ET7mFUgwOpQa1HAC749luN2aAju15GhKGIcBMUiMQoQKUwNPQV292A8VZGfUe4vYJIi7kirBfPFeRAL67A"

        ;
        while(true) {
            String s = HttpUtils.sendGetData("http://127.0.0.1:3080/api/no/id?access_token=" + token, new HashMap<>());
            JSONObject jsonObject = JSON.parseObject(s);
            if (null != jsonObject.get("error")) {
                break;
            }
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }
    @Test
    void test2() {
        String token =
"eyJraWQiOiIwYTJmMDgzZS1lMzZkLTQ0NjctYmFiYi1mMGM2MmVhN2M5ZjIiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiI1YzBlNzUwYjdhNWNkNDI0NjRhNTA5OWQiLCJhdWQiOiI1YzBlNzUwYjdhNWNkNDI0NjRhNTA5OWQiLCJjcmVhdGVkQXQiOjE3NTc2Njc1OTQwODksIm5iZiI6MTc1NzY2NzU5NCwiY2xpZW50SWQiOiI1YzBlNzUwYjdhNWNkNDI0NjRhNTA5OWQiLCJyb2xlcyI6WyIkZXZlcnlvbmUiLCJhZG1pbiJdLCJpc3MiOiJodHRwOi8vMTI3LjAuMC4xOjMwMDAiLCJleHBpcmVkYXRlIjoxNzU3NjY3ODk0MDg5LCJleHAiOjE3NTc2Njc4OTQsImlhdCI6MTc1NzY2NzU5NCwianRpIjoiNzY0ZGI1N2MtYmU3Ni00YmJmLTlmNDgtMmQ4N2ViMmQ4ZmFjIn0.IvVzcmDFWDJBwxgDBMTYJ-DEmNEHf2Gq77BRbd3zrfuTRiBUboKUhGMLqDVfC2E7i9WQdKaiaDa0uY2BS7huO586O3rt4dntlMcE334X1rMlIzf0i1DhJb-Ihz8b9E0_Dfqu93XVluO8L77KCaqyKLyIZTUzxK9YmFlCSbmeIRofEqOibLpFeRY6uxUOK5MwutVkqe_zOahgTdnklx7ClyzwFSsKrTpMPanJVjPS1oSWdP29t3zKhyoJkCpAhUPTkxu3ET7mFUgwOpQa1HAC749luN2aAju15GhKGIcBMUiMQoQKUwNPQV292A8VZGfUe4vYJIi7kirBfPFeRAL67A"

                ;
        while(true) {
            String s = HttpUtils.sendGetData("http://127.0.0.1:3080/api/x999/o9?access_token=" + token, new HashMap<>());
            JSONObject jsonObject = JSON.parseObject(s);
            if (null != jsonObject.get("error")) {
                break;
            }
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }
}
