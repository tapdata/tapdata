package io.tapdata.Schedule;

import base.BaseTest;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.*;
import com.tapdata.entity.values.CheckEngineValidResultDto;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.RestTemplateOperator;
import com.tapdata.tm.worker.WorkerSingletonLock;
import io.tapdata.common.SettingService;
import io.tapdata.entity.error.CoreException;
import io.tapdata.metric.MetricManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.internal.verification.Times;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.HttpClientErrorException;

import java.util.*;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;
import static org.springframework.data.mongodb.core.query.Criteria.where;

@RunWith(MockitoJUnitRunner.class)
class ConnectorManagerTest extends BaseTest {
    private ConnectorManager connectorManager = new ConnectorManager();
    @Nested
    class TestInitForCheckLicenseEngineLimit{
        private MongoTemplate mongoTemplate;
        private RestTemplateOperator restTemplateOperator;
        private List<String> baseURLs;
        @BeforeEach
        void buildConnectManager(){
            baseURLs = new ArrayList<>();
            baseURLs.add("url1");
            ReflectionTestUtils.setField(connectorManager,"baseURLs",baseURLs);
            ReflectionTestUtils.setField(connectorManager,"appType",AppType.DAAS);
            ReflectionTestUtils.setField(connectorManager,"tapdataWorkDir","c:/");
            ClientMongoOperator clientMongoOperator = spy(new ClientMongoOperator());
            mongoTemplate = mock(MongoTemplate.class);
            ReflectionTestUtils.setField(clientMongoOperator,"mongoTemplate",mongoTemplate);
            ReflectionTestUtils.setField(connectorManager,"clientMongoOperator",clientMongoOperator);
            ConfigurationCenter configCenter = mock(ConfigurationCenter.class);
            ReflectionTestUtils.setField(connectorManager,"configCenter",configCenter);
            restTemplateOperator = mock(RestTemplateOperator.class);
            ReflectionTestUtils.setField(connectorManager,"restTemplateOperator",restTemplateOperator);
        }
        @Test
        void testInitForThrowRuntimeEx(){
            baseURLs.clear();
            assertThrows(RuntimeException.class,()->connectorManager.init());
        }
        @Test
        void testInitForCheckLicenseEngineLimit() throws Exception {
            User user = mock(User.class);
            List<User> users = new ArrayList<>();
            users.add(user);
            Map<String, Object> params = new HashMap<>();
            params.put("accesscode", user.getAccesscode());
            when(mongoTemplate.find(new Query(where("role").is(1)), User.class, "User")).thenReturn(users);
            LoginResp resp = new LoginResp();
            ReflectionTestUtils.setField(resp,"created", "2024-02-12 20:55:09");
            ReflectionTestUtils.setField(resp,"ttl", 1111111L);
            when(restTemplateOperator.postOne(params, "users/generatetoken", LoginResp.class)).thenReturn(resp);
            SettingService settingService = mock(SettingService.class);
            ReflectionTestUtils.setField(connectorManager,"settingService",settingService);
            try (MockedStatic<WorkerSingletonLock> singletonLock = mockStatic(WorkerSingletonLock.class)){
                singletonLock.when(()->WorkerSingletonLock.check(any(),any())).thenAnswer(answer ->{
                    return null;
                });
                when(settingService.getSetting("buildProfile")).thenReturn(mock(Setting.class));
                connectorManager.init();
            }
        }
        @Test
        void testInitForCheckLicenseEngineLimitWithEx() throws Exception {
            User user = mock(User.class);
            List<User> users = new ArrayList<>();
            users.add(user);
            Map<String, Object> params = new HashMap<>();
            params.put("accesscode", user.getAccesscode());
            when(mongoTemplate.find(new Query(where("role").is(1)), User.class, "User")).thenReturn(users);
            LoginResp resp = new LoginResp();
            ReflectionTestUtils.setField(resp,"created", "2024-02-12 20:55:09");
            ReflectionTestUtils.setField(resp,"ttl", 1111111L);
            when(restTemplateOperator.postOne(params, "users/generatetoken", LoginResp.class)).thenReturn(resp);
            SettingService settingService = mock(SettingService.class);
            ReflectionTestUtils.setField(connectorManager,"settingService",settingService);
            try (MockedStatic<WorkerSingletonLock> singletonLock = mockStatic(WorkerSingletonLock.class)){
                singletonLock.when(()->WorkerSingletonLock.check(any(),any())).thenAnswer(answer ->{
                    return null;
                });
                CheckEngineValidResultDto resultDto = mock(CheckEngineValidResultDto.class);
                resultDto.setResult(false);
                when(connectorManager.checkLicenseEngineLimit()).thenReturn(resultDto);
                assertThrows(CoreException.class,()->connectorManager.init());
            }
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
        @Test
        void testCheckLicenseEngineLimitWithEx(){
            connectorManager = spy(ConnectorManager.class);
            ReflectionTestUtils.setField(connectorManager,"appType",AppType.DAAS);
            ClientMongoOperator clientMongoOperator = mock(ClientMongoOperator.class);
            ReflectionTestUtils.setField(connectorManager,"clientMongoOperator",clientMongoOperator);
            HttpClientErrorException ex = mock(HttpClientErrorException.class);
            Map<String, Object> processId = new HashMap<>();
            processId.put("processId", "tapdata-agent-connector");
            doThrow(ex).when(clientMongoOperator).findOne(processId, ConnectorConstant.LICENSE_COLLECTION + "/checkEngineValid", CheckEngineValidResultDto.class);
            assertEquals(null,connectorManager.checkLicenseEngineLimit());
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
