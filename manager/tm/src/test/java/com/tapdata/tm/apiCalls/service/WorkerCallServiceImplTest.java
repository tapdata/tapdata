package com.tapdata.tm.apiCalls.service;

import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.apiCalls.entity.WorkerCallStats;
import com.tapdata.tm.apiServer.service.compress.Compress;
import com.tapdata.tm.apiServer.service.compress.CompressMinute;
import com.tapdata.tm.apiServer.service.metric.Metric;
import com.tapdata.tm.apiServer.service.metric.MetricRPS;
import com.tapdata.tm.apiServer.vo.ApiCallMetricVo;
import com.tapdata.tm.apiCalls.vo.ApiCountMetricVo;
import com.tapdata.tm.apiCalls.vo.WorkerCallsInfo;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.worker.dto.ApiServerStatus;
import com.tapdata.tm.worker.dto.ApiServerWorkerInfo;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.ApiWorkerServer;
import com.tapdata.tm.worker.service.WorkerService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;

class WorkerCallServiceImplTest {
    WorkerCallServiceImpl callService;

    WorkerService workerService;
    MongoTemplate mongoOperations;
    ApiWorkerServer apiWorkerServer;

    @BeforeEach
    void init() {
        callService = mock(WorkerCallServiceImpl.class);
        workerService = mock(WorkerService.class);
        mongoOperations = mock(MongoTemplate.class);
        apiWorkerServer = mock(ApiWorkerServer.class);
        ReflectionTestUtils.setField(callService, "workerService", workerService);
        ReflectionTestUtils.setField(callService, "mongoOperations", mongoOperations);
        ReflectionTestUtils.setField(callService, "apiWorkerServer", apiWorkerServer);
    }

    @Nested
    class findTest {

        @Test
        void testNormal() {
            when(callService.find("id", 1L, 1L, 0, 1)).thenCallRealMethod();
            List<WorkerCallEntity> items = new ArrayList<>();
            WorkerCallEntity entity = new WorkerCallEntity();
            entity.setWorkOid("id");
            entity.setTimeStart(100L);
            items.add(entity);
            when(mongoOperations.find(any(Query.class), any(Class.class), anyString())).thenReturn(items);
            when(apiWorkerServer.getServerInfo(anyString())).thenReturn(mock(Worker.class));
            try (MockedStatic<Metric> ms = mockStatic(Metric.class);
                 MockedStatic<Compress> mc = mockStatic(Compress.class)) {
                ms.when(() -> Metric.call(0)).thenReturn(new MetricRPS());
                mc.when(() -> Compress.call(1)).thenReturn(new CompressMinute());
                ApiCallMetricVo vo = callService.find("id", 1L, 1L, 0, 1);
                Assertions.assertNotNull(vo);
            }
        }

        @Test
        void testQueryResultIsEmpty() {
            when(callService.find("id", 1L, 1L, 0, 1)).thenCallRealMethod();
            List<WorkerCallEntity> items = new ArrayList<>();
            when(mongoOperations.find(any(Query.class), any(Class.class), anyString())).thenReturn(items);
            when(apiWorkerServer.getServerInfo(anyString())).thenReturn(mock(Worker.class));
            try (MockedStatic<Metric> ms = mockStatic(Metric.class);
                 MockedStatic<Compress> mc = mockStatic(Compress.class)) {
                ms.when(() -> Metric.call(0)).thenReturn(new MetricRPS());
                mc.when(() -> Compress.call(1)).thenReturn(new CompressMinute());
                ApiCallMetricVo vo = callService.find("id", 1L, 1L, 0, 1);
                Assertions.assertNotNull(vo);
            }
        }

        @Test
        void testProcessIdIsBlank() {
            when(callService.find("", 1L, 1L, 0, 1)).thenCallRealMethod();
            Assertions.assertThrows(BizException.class, () -> {
                try {
                    callService.find("", 1L, 1L, 0, 1);
                } catch (BizException e) {
                    Assertions.assertEquals("api.call.metric.process.id.required", e.getErrorCode());
                    throw e;
                }
            });
        }
    }

    @Nested
    class findWorkerApiCallsTest {
        @Test
        void testNormal() {
            Worker server = new Worker();
            when(apiWorkerServer.getServerInfo(anyString())).thenReturn(server);
            Map<String, String> workerMap = new HashMap<>();
            when(apiWorkerServer.workerMap(server)).thenReturn(workerMap);
            when(callService.apiMap()).thenReturn(new HashMap<>());
            doNothing().when(callService).collectApiCall(any(WorkerCallStats.class), anyString(), anyMap(), anyMap());
            when(callService.findWorkerApiCalls("id")).thenCallRealMethod();
            List<WorkerCallStats> apiCallInWorker = new ArrayList<>();
            WorkerCallStats stats1 = new WorkerCallStats();
            apiCallInWorker.add(stats1);
            apiCallInWorker.add(null);
            WorkerCallStats stats2 = new WorkerCallStats();
            stats2.setAllPathId("id2");
            apiCallInWorker.add(stats2);
            when(callService.apiCallInWorkers(anyString())).thenReturn(apiCallInWorker);

            ApiCountMetricVo vo = callService.findWorkerApiCalls("id");
            Assertions.assertNotNull(vo);
        }

        @Test
        void testApiCallInWorkerIsEmpty() {
            Worker server = new Worker();
            when(apiWorkerServer.getServerInfo(anyString())).thenReturn(server);
            Map<String, String> workerMap = new HashMap<>();
            when(apiWorkerServer.workerMap(server)).thenReturn(workerMap);
            when(callService.apiMap()).thenReturn(new HashMap<>());
            doNothing().when(callService).collectApiCall(any(WorkerCallStats.class), anyString(), anyMap(), anyMap());
            when(callService.findWorkerApiCalls("id")).thenCallRealMethod();
            List<WorkerCallStats> apiCallInWorker = new ArrayList<>();
            when(callService.apiCallInWorkers(anyString())).thenReturn(apiCallInWorker);

            ApiCountMetricVo vo = callService.findWorkerApiCalls("id");
            Assertions.assertNotNull(vo);
        }

        @Test
        void testWorkerApiCountMapNotEmpty() {
            Worker server = new Worker();
            when(apiWorkerServer.getServerInfo(anyString())).thenReturn(server);
            Map<String, String> workerMap = new HashMap<>();
            when(apiWorkerServer.workerMap(server)).thenReturn(workerMap);
            when(callService.apiMap()).thenReturn(new HashMap<>());
            doAnswer(a -> {
                Map<String, ApiCountMetricVo.ApiItem> map = new HashMap<>(16);
                map.put("id", new ApiCountMetricVo.ApiItem());
                ((Map<String, Map<String, ApiCountMetricVo.ApiItem>>) a.getArgument(2)).put("id", map);
                return null;
            }).when(callService).collectApiCall(any(WorkerCallStats.class), anyString(), anyMap(), anyMap());
            when(callService.findWorkerApiCalls("id")).thenCallRealMethod();
            List<WorkerCallStats> apiCallInWorker = new ArrayList<>();
            WorkerCallStats stats1 = new WorkerCallStats();
            apiCallInWorker.add(stats1);
            apiCallInWorker.add(null);
            WorkerCallStats stats2 = new WorkerCallStats();
            stats2.setAllPathId("id2");
            apiCallInWorker.add(stats2);
            when(callService.apiCallInWorkers(anyString())).thenReturn(apiCallInWorker);

            ApiCountMetricVo vo = callService.findWorkerApiCalls("id");
            Assertions.assertNotNull(vo);
        }
    }

    @Nested
    class apiMapTest {
        @Test
        void testNormal() {
            when(callService.apiMap()).thenCallRealMethod();
            when(mongoOperations.find(any(Query.class), any(Class.class), anyString())).thenReturn(new ArrayList<>());
            Map<String, String> apiMap = callService.apiMap();
            Assertions.assertNotNull(apiMap);
        }

        @Test
        void testQueryResultNotEmpty() {
            when(callService.apiMap()).thenCallRealMethod();
            List<ModulesEntity> modules = new ArrayList<>();
            ModulesEntity entity = new ModulesEntity();
            entity.setName("name");
            entity.setId(new ObjectId());
            modules.add(entity);
            ModulesEntity entity1 = new ModulesEntity();
            entity1.setId(new ObjectId());
            modules.add(entity1);
            when(mongoOperations.find(any(Query.class), any(Class.class), anyString())).thenReturn(modules);
            Map<String, String> apiMap = callService.apiMap();
            Assertions.assertNotNull(apiMap);
        }
    }

    @Nested
    class collectApiCallTest {
        @Test
        void testNormal() {
            doNothing().when(callService).collectApiCallOnce(any(WorkerCallStats.class), anyString(), anyMap());
            doCallRealMethod().when(callService).collectApiCall(any(WorkerCallStats.class), anyString(), anyMap(), anyMap());
            WorkerCallStats apiCallStats = new WorkerCallStats();
            apiCallStats.setAllPathId("id");
            apiCallStats.setTotalCount(1L);
            apiCallStats.setNotOkCount(1L);
            Map<String, ApiCountMetricVo.ApiItem> apiCountMap = new HashMap<>();
            callService.collectApiCall(apiCallStats, "name", new HashMap<>(), apiCountMap);
            Assertions.assertNotNull(apiCountMap);
        }
    }

    @Nested
    class collectApiCallOnceTest {
        @Test
        void testNormal() {
            doCallRealMethod().when(callService).collectApiCallOnce(any(WorkerCallStats.class), anyString(), anyMap());
            WorkerCallStats apiCallStats = new WorkerCallStats();
            apiCallStats.setAllPathId("id");
            apiCallStats.setTotalCount(1L);
            apiCallStats.setNotOkCount(1L);
            Map<String, ApiCountMetricVo.ApiItem> apiCountMap = new HashMap<>();
            callService.collectApiCallOnce(apiCallStats, "name", apiCountMap);
            Assertions.assertNotNull(apiCountMap);
        }
    }

    @Nested
    class apiCallInWorkersTest {
        @Test
        void testNormal() {
            when(callService.apiCallInWorkers(anyString())).thenCallRealMethod();
            when(mongoOperations.find(any(Query.class), any(Class.class), anyString())).thenReturn(new ArrayList<>());
            List<WorkerCallStats> apiCallInWorker = callService.apiCallInWorkers("id");
            Assertions.assertNotNull(apiCallInWorker);
        }
    }

    @Nested
    class collectApiCallCountGroupByWorkerTest {
        @Test
        void testNormal() {
            List<WorkerCallStats> apiCallInWorker = new ArrayList<>();
            when(callService.apiCallInWorkers(anyString())).thenReturn(apiCallInWorker);
            ApiCallEntity topOne = new ApiCallEntity();
            topOne.setId(new ObjectId());
            when(mongoOperations.findOne(any(Query.class), any(Class.class))).thenReturn(topOne);
            List<ApiCallEntity> apiCalls = new ArrayList<>();
            when(mongoOperations.find(any(Query.class), any(Class.class), anyString())).thenReturn(apiCalls);
            BulkOperations bulkOps = mock(BulkOperations.class);
            when(bulkOps.upsert(any(Query.class), any(Update.class))).thenReturn(null);
            when(bulkOps.execute()).thenReturn(null);
            when(mongoOperations.bulkOps(BulkOperations.BulkMode.ORDERED, WorkerCallStats.class)).thenReturn(bulkOps);
            Map<String, Map<String, WorkerCallStats>> groupByApiAndWorker = new HashMap<>();
            when(callService.groupCallResult(anyString(), anyList())).thenReturn(groupByApiAndWorker);
            doCallRealMethod().when(callService).collectApiCallCountGroupByWorker(anyString());
            Assertions.assertDoesNotThrow(() -> callService.collectApiCallCountGroupByWorker("id"));
        }

        @Test
        void testApiCallInWorkerNotEmpty() {
            List<WorkerCallStats> apiCallInWorker = new ArrayList<>();
            WorkerCallStats s1 = new WorkerCallStats();
            apiCallInWorker.add(s1);
            apiCallInWorker.add(null);
            s1.setLastCallId(new ObjectId().toHexString());

            Map<String, Map<String, WorkerCallStats>> groupByApiAndWorker = new HashMap<>();
            when(callService.groupCallResult(anyString(), anyList())).thenReturn(groupByApiAndWorker);
            when(callService.apiCallInWorkers(anyString())).thenReturn(apiCallInWorker);
            ApiCallEntity topOne = new ApiCallEntity();
            topOne.setId(new ObjectId());
            when(mongoOperations.findOne(any(Query.class), any(Class.class))).thenReturn(topOne);
            List<ApiCallEntity> apiCalls = new ArrayList<>();
            when(mongoOperations.find(any(Query.class), any(Class.class), anyString())).thenReturn(apiCalls);
            BulkOperations bulkOps = mock(BulkOperations.class);
            when(bulkOps.upsert(any(Query.class), any(Update.class))).thenReturn(null);
            when(bulkOps.execute()).thenReturn(null);
            when(mongoOperations.bulkOps(BulkOperations.BulkMode.ORDERED, WorkerCallStats.class)).thenReturn(bulkOps);
            doCallRealMethod().when(callService).collectApiCallCountGroupByWorker(anyString());
            Assertions.assertDoesNotThrow(() -> callService.collectApiCallCountGroupByWorker("id"));
        }

        @Test
        void testTopOneIsNull() {
            List<WorkerCallStats> apiCallInWorker = new ArrayList<>();
            WorkerCallStats s1 = new WorkerCallStats();
            apiCallInWorker.add(s1);
            apiCallInWorker.add(null);
            s1.setLastCallId(new ObjectId().toHexString());

            Map<String, Map<String, WorkerCallStats>> groupByApiAndWorker = new HashMap<>();
            when(callService.groupCallResult(anyString(), anyList())).thenReturn(groupByApiAndWorker);
            when(callService.apiCallInWorkers(anyString())).thenReturn(apiCallInWorker);
            when(mongoOperations.findOne(any(Query.class), any(Class.class))).thenReturn(null);
            List<ApiCallEntity> apiCalls = new ArrayList<>();
            when(mongoOperations.find(any(Query.class), any(Class.class), anyString())).thenReturn(apiCalls);
            BulkOperations bulkOps = mock(BulkOperations.class);
            when(bulkOps.upsert(any(Query.class), any(Update.class))).thenReturn(null);
            when(bulkOps.execute()).thenReturn(null);
            when(mongoOperations.bulkOps(BulkOperations.BulkMode.ORDERED, WorkerCallStats.class)).thenReturn(bulkOps);
            doCallRealMethod().when(callService).collectApiCallCountGroupByWorker(anyString());
            Assertions.assertDoesNotThrow(() -> callService.collectApiCallCountGroupByWorker("id"));
        }

        @Test
        void testGroupByApiAndWorkerNotEmpty() {
            List<WorkerCallStats> apiCallInWorker = new ArrayList<>();
            WorkerCallStats s1 = new WorkerCallStats();
            apiCallInWorker.add(s1);
            apiCallInWorker.add(null);
            s1.setLastCallId(new ObjectId().toHexString());

            Map<String, Map<String, WorkerCallStats>> groupByApiAndWorker = new HashMap<>();
            Map<String, WorkerCallStats> map = new HashMap<>();
            map.put("1", s1);
            groupByApiAndWorker.put("id", map);
            when(callService.groupCallResult(anyString(), anyList())).thenReturn(groupByApiAndWorker);
            when(callService.apiCallInWorkers(anyString())).thenReturn(apiCallInWorker);
            ApiCallEntity topOne = new ApiCallEntity();
            topOne.setId(new ObjectId());
            when(mongoOperations.findOne(any(Query.class), any(Class.class))).thenReturn(topOne);
            List<ApiCallEntity> apiCalls = new ArrayList<>();
            when(mongoOperations.find(any(Query.class), any(Class.class), anyString())).thenReturn(apiCalls);
            BulkOperations bulkOps = mock(BulkOperations.class);
            when(bulkOps.upsert(any(Query.class), any(Update.class))).thenReturn(null);
            when(bulkOps.execute()).thenReturn(null);
            when(mongoOperations.bulkOps(BulkOperations.BulkMode.ORDERED, WorkerCallStats.class)).thenReturn(bulkOps);
            doCallRealMethod().when(callService).collectApiCallCountGroupByWorker(anyString());
            Assertions.assertDoesNotThrow(() -> callService.collectApiCallCountGroupByWorker("id"));
        }
    }

    @Nested
    class groupCallResultTest {
        @Test
        void testNormal() {
            when(callService.groupCallResult(anyString(), anyList())).thenCallRealMethod();
            List<ApiCallEntity> apiCalls = new ArrayList<>();
            Map<String, Map<String, WorkerCallStats>> groupByApiAndWorker = callService.groupCallResult("id", apiCalls);
            Assertions.assertNotNull(groupByApiAndWorker);
        }

        @Test
        void testApiCallsNotEmpty() {
            when(callService.groupCallResult(anyString(), anyList())).thenCallRealMethod();
            List<ApiCallEntity> apiCalls = new ArrayList<>();
            ApiCallEntity entity = new ApiCallEntity();
            apiCalls.add(entity);
            apiCalls.add(null);
            ApiCallEntity entity1 = new ApiCallEntity();
            entity1.setCodeMsg("ok");
            apiCalls.add(entity1);
            Map<String, Map<String, WorkerCallStats>> groupByApiAndWorker = callService.groupCallResult("id", apiCalls);
            Assertions.assertNotNull(groupByApiAndWorker);
        }
    }

    @Nested
    class metricTest {
        @Test
        void testNormal() {
            List<WorkerDto> apiServers = new ArrayList<>();
            WorkerDto d4 = new WorkerDto();
            ApiServerStatus map1 = new ApiServerStatus();
            Map<String, ApiServerWorkerInfo> info = new HashMap<>();
            map1.setWorkers(info);
            ApiServerWorkerInfo workerInfo = new ApiServerWorkerInfo();
            workerInfo.setOid(new ObjectId().toHexString());
            info.put("1", workerInfo);
            d4.setWorkerStatus(map1);
            apiServers.add(d4);

            when(workerService.findAll(any(Query.class))).thenReturn(apiServers);
            doNothing().when(callService).metricWorker(anyString());
            doCallRealMethod().when(callService).metric();
            Assertions.assertDoesNotThrow(() -> callService.metric());
        }
        @Test
        void testNotOid() {
            List<WorkerDto> apiServers = new ArrayList<>();
            WorkerDto d4 = new WorkerDto();
            ApiServerStatus map1 = new ApiServerStatus();
            Map<String, ApiServerWorkerInfo> info = new HashMap<>();
            map1.setWorkers(info);
            ApiServerWorkerInfo workerInfo = new ApiServerWorkerInfo();
            info.put("1", workerInfo);
            d4.setWorkerStatus(map1);
            apiServers.add(d4);

            when(workerService.findAll(any(Query.class))).thenReturn(apiServers);
            doNothing().when(callService).metricWorker(anyString());
            doCallRealMethod().when(callService).metric();
            Assertions.assertDoesNotThrow(() -> callService.metric());
        }

        @Test
        void testNull() {
            List<WorkerDto> apiServers = new ArrayList<>();
            apiServers.add(null);
            when(workerService.findAll(any(Query.class))).thenReturn(apiServers);
            doNothing().when(callService).metricWorker(anyString());
            doCallRealMethod().when(callService).metric();
            Assertions.assertDoesNotThrow(() -> callService.metric());
        }

        @Test
        void testNotWorkerInfo() {
            List<WorkerDto> apiServers = new ArrayList<>();
            WorkerDto d4 = new WorkerDto();
            ApiServerStatus map1 = new ApiServerStatus();
            Map<String, ApiServerWorkerInfo> info = new HashMap<>();
            map1.setWorkers(info);
            ApiServerWorkerInfo workerInfo = new ApiServerWorkerInfo();
            workerInfo.setOid(new ObjectId().toHexString());
            info.put("2", workerInfo);
            d4.setWorkerStatus(map1);
            apiServers.add(d4);

            when(workerService.findAll(any(Query.class))).thenReturn(apiServers);
            doNothing().when(callService).metricWorker(anyString());
            doCallRealMethod().when(callService).metric();
            Assertions.assertDoesNotThrow(() -> callService.metric());
        }
    }

    @Nested
    class bulkUpsertTest {
        @Test
        void testNormal() {
            List<WorkerCallEntity> entities = new ArrayList<>();
            WorkerCallEntity e1 = new WorkerCallEntity();
            entities.add(e1);
            doCallRealMethod().when(callService).bulkUpsert(anyList());
            doCallRealMethod().when(callService).bulkUpsert(anyList(), any(Function.class), any(Function.class));
            BulkOperations bulkOps = mock(BulkOperations.class);
            when(mongoOperations.bulkOps(BulkOperations.BulkMode.ORDERED, WorkerCallEntity.class)).thenReturn(bulkOps);
            when(bulkOps.upsert(any(Query.class), any(Update.class))).thenReturn(null);
            when(bulkOps.execute()).thenReturn(null);
            Assertions.assertDoesNotThrow(() -> callService.bulkUpsert(entities));
        }

        @Test
        void testNullWorkerCallEntityList() {
            doCallRealMethod().when(callService).bulkUpsert(null);
            doCallRealMethod().when(callService).bulkUpsert(any(), any(Function.class), any(Function.class));
            BulkOperations bulkOps = mock(BulkOperations.class);
            when(mongoOperations.bulkOps(BulkOperations.BulkMode.ORDERED, WorkerCallEntity.class)).thenReturn(bulkOps);
            when(bulkOps.upsert(any(Query.class), any(Update.class))).thenReturn(null);
            when(bulkOps.execute()).thenReturn(null);
            Assertions.assertDoesNotThrow(() -> callService.bulkUpsert(null));
        }

        @Test
        void testEmptyWorkerCallEntityList() {
            doCallRealMethod().when(callService).bulkUpsert(anyList());
            doCallRealMethod().when(callService).bulkUpsert(anyList(), any(Function.class), any(Function.class));
            BulkOperations bulkOps = mock(BulkOperations.class);
            when(mongoOperations.bulkOps(BulkOperations.BulkMode.ORDERED, WorkerCallEntity.class)).thenReturn(bulkOps);
            when(bulkOps.upsert(any(Query.class), any(Update.class))).thenReturn(null);
            when(bulkOps.execute()).thenReturn(null);
            Assertions.assertDoesNotThrow(() -> callService.bulkUpsert(new ArrayList<>()));
        }
    }

    @Nested
    class metricWorkerTest {
        @Test
        void testNormal() {
            doCallRealMethod().when(callService).metricWorker(anyString());
            WorkerCallEntity lastOne = new WorkerCallEntity();
            lastOne.setTimeStart(System.currentTimeMillis());
            WorkerCallsInfo workerCallsInfo = new WorkerCallsInfo();
            workerCallsInfo.setWorkOid(new ObjectId().toHexString());
            workerCallsInfo.setLatency(100L);
            workerCallsInfo.setCode("200");
            workerCallsInfo.setResTime(System.currentTimeMillis());
            workerCallsInfo.setReqTime(System.currentTimeMillis());
            workerCallsInfo.setApiGatewayUuid(new ObjectId().toHexString());
            when(mongoOperations.find(any(Query.class), any(Class.class), anyString())).thenReturn(List.of(workerCallsInfo), new ArrayList<>());
            when(mongoOperations.findOne(any(Query.class), any(Class.class))).thenReturn(lastOne);
            doNothing().when(callService).bulkUpsert(anyList());
            Assertions.assertDoesNotThrow(() -> callService.metricWorker("id"));
        }
        @Test
        void testLastOneIsNull() {
            doCallRealMethod().when(callService).metricWorker(anyString());
            when(mongoOperations.find(any(Query.class), any(Class.class), anyString())).thenReturn(new ArrayList<>());
            when(mongoOperations.findOne(any(Query.class), any(Class.class))).thenReturn(null);
            doNothing().when(callService).bulkUpsert(anyList());
            Assertions.assertDoesNotThrow(() -> callService.metricWorker("id"));
        }
    }

    @Nested
    class findDataTest {
        @Test
        void testNormal() {
            when(callService.findData(any(Query.class))).thenCallRealMethod();
            when(mongoOperations.find(any(Query.class), any(Class.class))).thenReturn(new ArrayList<>());
            List<WorkerCallEntity> data = callService.findData(Query.query(Criteria.where("workOid").is("id")));
            Assertions.assertNotNull(data);
        }
    }
}