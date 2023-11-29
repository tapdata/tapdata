package io.tapdata.Schedule;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.DateUtil;
import com.tapdata.entity.AppType;
import com.tapdata.entity.LoginResp;
import com.tapdata.entity.User;
import com.tapdata.entity.Worker;
import com.tapdata.entity.values.CheckEngineValidResultDto;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.RestTemplateOperator;
import com.tapdata.tm.worker.WorkerSingletonLock;
import io.tapdata.common.SettingService;
import io.tapdata.metric.MetricManager;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.internal.verification.Times;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.query.Criteria.where;

@RunWith(MockitoJUnitRunner.class)
class ConnectorManagerTest {
    private ConnectorManager connectorManager = new ConnectorManager();
    @Nested
    class TestInit{
//        @Test
        void testInit() throws Exception {
            List<String> baseURLs = new ArrayList<>();
            baseURLs.add("url1");
            ReflectionTestUtils.setField(connectorManager,"baseURLs",baseURLs);
            ReflectionTestUtils.setField(connectorManager,"appType",AppType.DAAS);
            ReflectionTestUtils.setField(connectorManager,"tapdataWorkDir","c:/");
            ClientMongoOperator clientMongoOperator = spy(new ClientMongoOperator());
            MongoTemplate mongoTemplate = mock(MongoTemplate.class);
            ReflectionTestUtils.setField(clientMongoOperator,"mongoTemplate",mongoTemplate);
            ReflectionTestUtils.setField(connectorManager,"clientMongoOperator",clientMongoOperator);
            ConfigurationCenter configCenter = mock(ConfigurationCenter.class);
            ReflectionTestUtils.setField(connectorManager,"configCenter",configCenter);
            RestTemplateOperator restTemplateOperator = mock(RestTemplateOperator.class);
            ReflectionTestUtils.setField(connectorManager,"restTemplateOperator",restTemplateOperator);
            User user = mock(User.class);
            List<User> users = new ArrayList<>();
            users.add(user);
            Map<String, Object> params = new HashMap<>();
            params.put("accesscode", user.getAccesscode());
            when(mongoTemplate.find(new Query(where("role").is(1)), User.class, "User")).thenReturn(users);
            LoginResp resp = new LoginResp();
            ReflectionTestUtils.setField(resp,"created", "2020-02-12 20:55:09");
            ReflectionTestUtils.setField(resp,"ttl", 1111111L);
            when(restTemplateOperator.postOne(params, "users/generatetoken", LoginResp.class)).thenReturn(resp);
//            doNothing().when(connectorManager).login();
            SettingService settingService = mock(SettingService.class);
            ReflectionTestUtils.setField(connectorManager,"settingService",settingService);
            ReflectionTestUtils.setField(connectorManager,"tapdataWorkDir","test");
            Function<String, String> call =
            (singletonLock) -> {
                String newSingletonLock;
                String singletonLockFromEnv = System.getenv("singletonLock");
                if (StringUtils.isNotBlank(singletonLockFromEnv)) {
                    // Cloud 全托管，永远使用环境变量中提供的 lock
                    newSingletonLock = singletonLockFromEnv;
                } else {
                    newSingletonLock = UUID.randomUUID().toString();
                }
                String status = clientMongoOperator.upsert(new HashMap<String, Object>() {{
                    put("process_id", "tapdata-agent-connector");
                    put("worker_type", ConnectorConstant.WORKER_TYPE_CONNECTOR);
                    put("singletonLock", singletonLock);
                }}, new HashMap<String, Object>() {{
                    put("singletonLock", newSingletonLock);
                }}, ConnectorConstant.WORKER_COLLECTION + "/singleton-lock", String.class);
                if (!"ok".equals(status)) {
                    throw new RuntimeException(String.format("Singleton check in remote failed: '%s'", status));
                }
                return newSingletonLock;
            };
            try (MockedStatic<WorkerSingletonLock> singletonLock = mockStatic(WorkerSingletonLock.class)){
                singletonLock.when(()->WorkerSingletonLock.check(any(),any())).thenAnswer(answer ->{
                    return null;
                });
            }
            connectorManager.init();
            verify(connectorManager, new Times(1)).init();
        }
    }
    @Nested
    class TestCheckLicenseEngineLimit{
        @Test
        void testCheckLicenseEngineLimitWithCloud(){
            connectorManager = mock(ConnectorManager.class);
            ReflectionTestUtils.setField(connectorManager,"appType",AppType.DFS);
            CheckEngineValidResultDto actual = connectorManager.checkLicenseEngineLimit();
            assertEquals(null,actual);
        }
        @Test
        void testCheckLicenseEngineLimitWithDaas(){
            connectorManager = spy(ConnectorManager.class);
            ReflectionTestUtils.setField(connectorManager,"appType",AppType.DAAS);
            ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
            ReflectionTestUtils.setField(connectorManager,"clientMongoOperator",clientMongoOperator);
            CheckEngineValidResultDto excepted = mock(CheckEngineValidResultDto.class);
            when(connectorManager.checkLicenseEngineLimit()).thenReturn(excepted);
            CheckEngineValidResultDto actual = connectorManager.checkLicenseEngineLimit();
            assertEquals(excepted,actual);
        }
    }
    @Nested
    class TestWorkerHeartBeat{
        @Test
        void testWorkerHeartBeat(){
            connectorManager = spy(ConnectorManager.class);
            ConfigurationCenter configCenter = mock(ConfigurationCenter.class);
            ReflectionTestUtils.setField(connectorManager,"configCenter",configCenter);
            SettingService settingService = mock(SettingService.class);
            ReflectionTestUtils.setField(connectorManager,"settingService",settingService);
            ClientMongoOperator pingClientMongoOperator = mock(ClientMongoOperator.class);
            ReflectionTestUtils.setField(connectorManager,"pingClientMongoOperator",pingClientMongoOperator);
            MetricManager metricManager = mock(MetricManager.class);
            WorkerSingletonLock workerSingletonLock = mock(WorkerSingletonLock.class);
            WorkerSingletonLock instance = mock(WorkerSingletonLock.class);
            ReflectionTestUtils.setField(workerSingletonLock,"instance",instance);
            try (MockedStatic<WorkerSingletonLock> singletonLock = mockStatic(WorkerSingletonLock.class)){
                singletonLock.when(WorkerSingletonLock::getCurrentTag).thenReturn("tag");
            }
            ReflectionTestUtils.setField(connectorManager,"metricManager",metricManager);
            ReflectionTestUtils.setField(connectorManager,"appType",AppType.DAAS);
            ReflectionTestUtils.setField(connectorManager,"appType",AppType.DAAS);
            //isExit true --> false
            List<Worker> workers = new ArrayList<>();
            Consumer<Boolean> beforeExit = mock(Consumer.class);
            beforeExit.accept(true);
            doAnswer(answer -> {
                beforeExit.accept(true);
                return null;
            }).when(connectorManager).checkAndExit(workers,beforeExit);
            connectorManager.workerHeartBeat();
            verify(connectorManager).workerHeartBeat();
        }
    }
    @Nested
    class TestCheckAndExit{
        @Test
        void testCheckAndExitWithEmptyWorkers(){
            connectorManager = spy(ConnectorManager.class);
            List<Worker> workers = new ArrayList<>();
            Consumer<Boolean> beforeExit = mock(Consumer.class);
            connectorManager.checkAndExit(workers,beforeExit);
            // 使用 ArgumentCaptor 捕获 Consumer 的参数
            ArgumentCaptor<Boolean> captor = ArgumentCaptor.forClass(Boolean.class);
            verify(beforeExit).accept(captor.capture());
            assertEquals(false, captor.getValue());
        }
        @Test
        void testCheckAndExitWithDFSDeleted(){
            connectorManager = spy(ConnectorManager.class);
            ReflectionTestUtils.setField(connectorManager,"appType",AppType.DFS);
            Worker worker = new Worker();
            worker.setDeleted(true);
            List<Worker> workers = new ArrayList<>();
            workers.add(worker);
            Consumer<Boolean> beforeExit = mock(Consumer.class);
            connectorManager.checkAndExit(workers,beforeExit);
            ArgumentCaptor<Boolean> captor = ArgumentCaptor.forClass(Boolean.class);
            verify(beforeExit).accept(captor.capture());
            assertEquals(true, captor.getValue());
        }
        @Test
        void testCheckAndExitWithDFSStopping(){
            connectorManager = spy(ConnectorManager.class);
            ReflectionTestUtils.setField(connectorManager,"appType",AppType.DFS);
            Worker worker = new Worker();
            worker.setStopping(true);
            List<Worker> workers = new ArrayList<>();
            workers.add(worker);
            Consumer<Boolean> beforeExit = mock(Consumer.class);
            connectorManager.checkAndExit(workers,beforeExit);
            ArgumentCaptor<Boolean> captor = ArgumentCaptor.forClass(Boolean.class);
            verify(beforeExit).accept(captor.capture());
            assertEquals(true, captor.getValue());
        }
        @Test
        void testCheckAndExitWithDAAS(){
            connectorManager = spy(ConnectorManager.class);
            ReflectionTestUtils.setField(connectorManager,"appType",AppType.DAAS);
            Worker worker = new Worker();
            List<Worker> workers = new ArrayList<>();
            workers.add(worker);
            Consumer<Boolean> beforeExit = mock(Consumer.class);
            CheckEngineValidResultDto resultDto = new CheckEngineValidResultDto();
            resultDto.setResult(false);
            ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
            ReflectionTestUtils.setField(connectorManager,"clientMongoOperator",clientMongoOperator);
            when(connectorManager.checkLicenseEngineLimit()).thenReturn(resultDto);
            connectorManager.checkAndExit(workers,beforeExit);
            ArgumentCaptor<Boolean> captor = ArgumentCaptor.forClass(Boolean.class);
            verify(beforeExit).accept(captor.capture());
            assertEquals(true, captor.getValue());
        }
        @Test
        void testCheckAndExitWithDAASId(){
            connectorManager = spy(ConnectorManager.class);
            ReflectionTestUtils.setField(connectorManager,"appType",AppType.DAAS);
            Worker worker = new Worker();
            List<Worker> workers = new ArrayList<>();
            workers.add(worker);
            Consumer<Boolean> beforeExit = mock(Consumer.class);
            CheckEngineValidResultDto resultDto = new CheckEngineValidResultDto();
            resultDto.setResult(false);
            resultDto.setProcessId("111");
            ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
            ReflectionTestUtils.setField(connectorManager,"clientMongoOperator",clientMongoOperator);
            when(connectorManager.checkLicenseEngineLimit()).thenReturn(resultDto);
            connectorManager.checkAndExit(workers,beforeExit);
            ArgumentCaptor<Boolean> captor = ArgumentCaptor.forClass(Boolean.class);
            verify(beforeExit).accept(captor.capture());
            assertEquals(true, captor.getValue());
        }
        @Test
        void testCheckAndExitWithDRS(){
            connectorManager = spy(ConnectorManager.class);
            ReflectionTestUtils.setField(connectorManager,"appType",AppType.DRS);
            Worker worker = new Worker();
            List<Worker> workers = new ArrayList<>();
            workers.add(worker);
            Consumer<Boolean> beforeExit = mock(Consumer.class);
            connectorManager.checkAndExit(workers,beforeExit);
            ArgumentCaptor<Boolean> captor = ArgumentCaptor.forClass(Boolean.class);
            verify(beforeExit).accept(captor.capture());
            assertEquals(false, captor.getValue());
        }
    }
}
