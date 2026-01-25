package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.cluster.repository.ClusterStateRepository;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.modules.constant.ModuleStatusEnum;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.utils.ApiMetricsDelayUtil;
import com.tapdata.tm.utils.HttpUtils;
import com.tapdata.tm.v2.api.monitor.main.dto.ApiDetail;
import com.tapdata.tm.v2.api.monitor.main.dto.ApiOfEachServer;
import com.tapdata.tm.v2.api.monitor.main.dto.ChartAndDelayOfApi;
import com.tapdata.tm.v2.api.monitor.main.dto.ServerChart;
import com.tapdata.tm.v2.api.monitor.main.dto.ServerItem;
import com.tapdata.tm.v2.api.monitor.main.dto.ServerOverviewDetail;
import com.tapdata.tm.v2.api.monitor.main.dto.ServerTopOnHomepage;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import com.tapdata.tm.v2.api.monitor.main.enums.TimeGranularity;
import com.tapdata.tm.v2.api.monitor.main.param.ApiChart;
import com.tapdata.tm.v2.api.monitor.main.param.ApiDetailParam;
import com.tapdata.tm.v2.api.monitor.main.param.ApiWithServerDetail;
import com.tapdata.tm.v2.api.monitor.main.param.QueryBase;
import com.tapdata.tm.v2.api.monitor.main.param.ServerChartParam;
import com.tapdata.tm.v2.api.monitor.main.param.ServerDetail;
import com.tapdata.tm.v2.api.monitor.main.param.ServerListParam;
import com.tapdata.tm.v2.api.monitor.main.param.TopWorkerInServerParam;
import com.tapdata.tm.v2.api.monitor.utils.ApiMetricsDelayInfoUtil;
import com.tapdata.tm.v2.api.usage.repository.ServerUsageMetricRepository;
import com.tapdata.tm.v2.api.usage.repository.UsageRepository;
import com.tapdata.tm.worker.dto.ApiServerStatus;
import com.tapdata.tm.worker.dto.ApiServerWorkerInfo;
import com.tapdata.tm.worker.dto.MetricInfo;
import com.tapdata.tm.worker.entity.ServerUsage;
import com.tapdata.tm.worker.entity.ServerUsageMetric;
import com.tapdata.tm.worker.entity.UsageBase;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.repository.WorkerRepository;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class ApiMetricsChartQueryTest {
    private ApiMetricsRawService service;
    private UsageRepository usageRepository;
    private WorkerRepository workerRepository;
    private ClusterStateRepository clusterRepository;
    private ModulesService modulesService;
    private MongoTemplate mongoTemplate;
    private ServerUsageMetricRepository serverUsageMetricRepository;
    private ApiMetricsChartQuery apiMetricsChartQuery;

    @BeforeEach
    void setUp() {
        service = mock(ApiMetricsRawService.class);
        usageRepository = mock(UsageRepository.class);
        workerRepository = mock(WorkerRepository.class);
        clusterRepository = mock(ClusterStateRepository.class);
        modulesService = mock(ModulesService.class);
        mongoTemplate = mock(MongoTemplate.class);
        serverUsageMetricRepository = mock(ServerUsageMetricRepository.class);

        apiMetricsChartQuery = mock(ApiMetricsChartQuery.class);
        ReflectionTestUtils.setField(apiMetricsChartQuery, "service", service);
        ReflectionTestUtils.setField(apiMetricsChartQuery, "usageRepository", usageRepository);
        ReflectionTestUtils.setField(apiMetricsChartQuery, "workerRepository", workerRepository);
        ReflectionTestUtils.setField(apiMetricsChartQuery, "clusterRepository", clusterRepository);
        ReflectionTestUtils.setField(apiMetricsChartQuery, "modulesService", modulesService);
        ReflectionTestUtils.setField(apiMetricsChartQuery, "serverUsageMetricRepository", serverUsageMetricRepository);
        when(apiMetricsChartQuery.serverTopOnHomepage(any(QueryBase.class))).thenCallRealMethod();
        //when(apiMetricsChartQuery.errorCount(anyList(), any(Function.class))).thenCallRealMethod();
        when(apiMetricsChartQuery.serverOverviewList(any(ServerListParam.class))).thenCallRealMethod();
        when(apiMetricsChartQuery.queryCpuUsageRecords(any(Criteria.class), anyLong(), anyLong(), any(TimeGranularity.class))).thenCallRealMethod();
        when(apiMetricsChartQuery.mapUsage(anyList(), anyLong(), anyLong(), any(TimeGranularity.class))).thenCallRealMethod();
        doCallRealMethod().when(apiMetricsChartQuery).asServerItemInfo(anyString(), any(ServerItem.class), anyMap(), any(Worker.class), anyMap(), any(ServerListParam.class));
        when(apiMetricsChartQuery.findServerById(anyString())).thenCallRealMethod();
        when(apiMetricsChartQuery.serverOverviewDetail(any(ServerDetail.class))).thenCallRealMethod();
        when(apiMetricsChartQuery.serverChart(any(ServerChartParam.class))).thenCallRealMethod();
        when(apiMetricsChartQuery.topWorkerInServer(any(TopWorkerInServerParam.class))).thenCallRealMethod();
        when(apiMetricsChartQuery.apiOverviewDetail(any(ApiDetailParam.class))).thenCallRealMethod();
        when(apiMetricsChartQuery.findRowByApiId(anyString(), any(QueryBase.class), any(MetricTypes.class), any(String[].class))).thenCallRealMethod();
        when(apiMetricsChartQuery.apiOfEachServer(any(ApiWithServerDetail.class))).thenCallRealMethod();
        when(apiMetricsChartQuery.delayOfApi(any(ApiChart.class))).thenCallRealMethod();
        when(apiMetricsChartQuery.extractIndex(anyString())).thenCallRealMethod();
        List<ModulesDto> modulesDtoLit = new ArrayList<>();
        ModulesDto m = new ModulesDto();
        m.setId(new ObjectId());
        modulesDtoLit.add(m);
        when(modulesService.findAllActiveApi(ModuleStatusEnum.ACTIVE)).thenReturn(modulesDtoLit);
        when(apiMetricsChartQuery.activeWorkers(anyList())).thenCallRealMethod();
        when(apiMetricsChartQuery.activeWorkers(anySet())).thenCallRealMethod();
        when(apiMetricsChartQuery.activeWorkers(null)).thenCallRealMethod();
        List<Worker> workers = new ArrayList<>();
        Worker e = new Worker();
        e.setProcessId("xxxxx");
        workers.add(e);
        when(workerRepository.findAll(any(Query.class))).thenReturn(workers);

    }

    @Nested
    class FindServerByIdTest {
        @Test
        void testFindServerById() {
            String serverId = "server1";
            Worker worker = createWorker(serverId, "hostname1");
            when(workerRepository.findOne(any(Query.class))).thenReturn(Optional.of(worker));

            Worker result = apiMetricsChartQuery.findServerById(serverId);

            assertNotNull(result);
            assertEquals(serverId, result.getProcessId());
        }

        @Test
        void testFindServerByIdNotFound() {
            String serverId = "server1";
            when(workerRepository.findOne(any(Query.class))).thenReturn(Optional.empty());

            assertThrows(BizException.class, () -> apiMetricsChartQuery.findServerById(serverId));
        }
    }

    @Nested
    class ServerOverviewDetailTest {
        @Test
        void testEmptyServerId() {
            ServerDetail param = new ServerDetail();
            param.setServerId("");

            assertThrows(BizException.class, () -> apiMetricsChartQuery.serverOverviewDetail(param));
        }

        @Test
        void testNullServerId() {
            ServerDetail param = new ServerDetail();
            param.setServerId(null);

            assertThrows(BizException.class, () -> apiMetricsChartQuery.serverOverviewDetail(param));
        }
    }

    @Nested
    class ServerChartTest {
        @Test
        void testEmptyServerId() {
            ServerChartParam param = new ServerChartParam();
            param.setServerId("");

            assertThrows(BizException.class, () -> apiMetricsChartQuery.serverChart(param));
        }
    }

    @Nested
    class TopWorkerInServerTest {
        @Test
        void testEmptyServerId() {
            TopWorkerInServerParam param = new TopWorkerInServerParam();
            param.setServerId("");
            assertThrows(BizException.class, () -> apiMetricsChartQuery.topWorkerInServer(param));
        }
    }

    @Nested
    class FindRowByApiIdTest {

        @Test
        void testNullApiId() {
            Criteria criteria = new Criteria();
            QueryBase param = new QueryBase();
            when(apiMetricsChartQuery.findRowByApiId(null, param, MetricTypes.API, new String[]{})).thenCallRealMethod();
            assertThrows(BizException.class, () -> apiMetricsChartQuery.findRowByApiId(null, param, MetricTypes.API, new String[]{}));
        }
    }

    @Nested
    class MapUsageTest {
        @Test
        void testNormal() {
            List<? extends ServerUsage> infos = new ArrayList<>();
            long startAt = 1765209600L;
            long endAt = 1765209660L;
            ServerChart.Usage usage = apiMetricsChartQuery.mapUsage(infos, startAt, endAt, TimeGranularity.SECOND_FIVE);
            Assertions.assertNotNull(usage);
            Assertions.assertNotNull(usage.getCpuUsage());
            Assertions.assertNotNull(usage.getMemoryUsage());
            Assertions.assertNotNull(usage.getTs());
            Assertions.assertEquals(0, usage.getTs().size());
        }

        @Test
        void testMinute() {
            List<? extends ServerUsage> infos = new ArrayList<>();
            long startAt = 1765209600L;
            long endAt = 1765213200L;
            ServerChart.Usage usage = apiMetricsChartQuery.mapUsage(infos, startAt, endAt, TimeGranularity.MINUTE);
            Assertions.assertNotNull(usage);
            Assertions.assertNotNull(usage.getCpuUsage());
            Assertions.assertNotNull(usage.getMemoryUsage());
            Assertions.assertNotNull(usage.getTs());
            Assertions.assertEquals(0, usage.getTs().size());
        }

        @Test
        void testHour() {
            List<? extends ServerUsage> infos = new ArrayList<>();
            long startAt = 1765209600L;
            long endAt = 1767888000L;
            ServerChart.Usage usage = apiMetricsChartQuery.mapUsage(infos, startAt, endAt, TimeGranularity.HOUR);
            Assertions.assertNotNull(usage);
            Assertions.assertNotNull(usage.getCpuUsage());
            Assertions.assertNotNull(usage.getMemoryUsage());
            Assertions.assertNotNull(usage.getTs());
            Assertions.assertEquals(0, usage.getTs().size());
        }
    }

    @Nested
    class extractIndexTest {
        @Test
        void testNormal() {
            Assertions.assertEquals(0, apiMetricsChartQuery.extractIndex("Worker-0"));
            Assertions.assertEquals(1, apiMetricsChartQuery.extractIndex("Worker-1"));
            Assertions.assertEquals(1, apiMetricsChartQuery.extractIndex("Worker---1"));
            Assertions.assertEquals(Integer.MAX_VALUE, apiMetricsChartQuery.extractIndex("Worker1"));
            Assertions.assertEquals(Integer.MAX_VALUE, apiMetricsChartQuery.extractIndex("Worker---"));
            Assertions.assertEquals(Integer.MAX_VALUE, apiMetricsChartQuery.extractIndex(""));
            Assertions.assertEquals(Integer.MAX_VALUE, apiMetricsChartQuery.extractIndex("Worker-ddd"));
        }
    }

    // Helper methods
    private ApiMetricsRaw createApiMetricsRaw(String apiId, String processId, Long reqCount, Long errorCount) {
        ApiMetricsRaw raw = new ApiMetricsRaw();
        raw.setApiId(apiId);
        raw.setProcessId(processId);
        raw.setReqCount(reqCount);
        raw.setErrorCount(errorCount);
        raw.setTimeStart(System.currentTimeMillis());
        raw.setDelay(new ArrayList<>());
        raw.setBytes(new ArrayList<>());
        raw.setRps(10.0);
        return raw;
    }

    private Worker createWorker(String processId, String hostname) {
        Worker worker = new Worker();
        worker.setProcessId(processId);
        worker.setHostname(hostname);
        return worker;
    }

    //@Test
    void call() {
        String token = "eyJraWQiOiI5NGJhMDRkNC0wYWZjLTRmNzgtYjAyMi1kZTAwNGQ1ZTlmNmIiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiI1YzBlNzUwYjdhNWNkNDI0NjRhNTA5OWQiLCJjbHVzdGVyIjoiNjk2MGJkZmM5YjhhODM1MDU0OWFjY2NiIiwiY2xpZW50SWQiOiI1YzBlNzUwYjdhNWNkNDI0NjRhNTA5OWQiLCJyb2xlcyI6WyIkZXZlcnlvbmUiLCJhZG1pbiJdLCJpc3MiOiJodHRwOi8vMTI3LjAuMC4xOjMwMDAiLCJleHBpcmVkYXRlIjoxMTUzNDMyMzcwODgxOTgsImF1ZCI6IjVjMGU3NTBiN2E1Y2Q0MjQ2NGE1MDk5ZCIsImNyZWF0ZWRBdCI6MTc2ODcwOTA4ODE5OCwibmJmIjoxNzY4NzA5MDg4LCJleHAiOjExNTM0MzIzNzA4OCwiaWF0IjoxNzY4NzA5MDg4LCJqdGkiOiJkMjUzNWJhMS01NDQ0LTRiZjItYjRkNS0yNjk1YWFmNGVjNWIifQ.U7Jg-FQ9zdae-dinCuy86383raAN150sl77MDClJysaamET_ozXXPIK0EM9bAyOnQswYEeVlbx1h9usuB9r4V3ANRPjhdEocW1TUeQHQjXGaC0htwWVpw7yjXiMz0UPc56aNVBNeCLo9xVKK4-YntjuU5TBvh4oM_m-DTVwTkXUnz4P8CBIixW1FaRAiR6gvKu6k3o20qVRvBj9U9HX2z_wPSLpY2GHGJwpQ3A-SMCtFPqW5Sy5ULkQ5TiEHA6PEZ-8FGI67SOrlqOjPm_WKMU3kAtNCD51X2SVIMO9466H5kW1qHVk_pmVE67eIMJI3l05L4-Ar0OeGPCf79FHuJg";
        String uri = "http://127.0.0.1:3080/api/%s?access_token=%s";
        List<String> api = List.of("v1/tjq7duqpvs7", "v1/aslw80no7ze", "v1/tnuihy78hd1", "v1/c2hhm58iqvf",
                "/v1/we/y36xqmi0k0i", "/v1/sd/qq", "/v1/a7gei772p62", "/v1/tjq7duqpvs7", "/v1/aslw80no7ze", "/v1/tnuihy78hd1",
                "/v1/c2hhm58iqvf", "/v1/v1v1lerdf18", "/v1/zohp6j9z28a", "/v1/call/ekwoyltbqit", "/v1/call/fiyh6xusf8w", "/v1/a56jflpyrs8",
                "/v1/mbooiyue1w9", "/v1/po73y0ge6e7", "/v1/call/yt2cpjuhyfr", "/v1/call/fields", "/v1/jadevt6nzpm",
                "/v1/yw0n3lvjiku", "/v1/aqs919theqr", "/v1/agvegbzt3qx", "/v1/qrknw3gxn5c", "/v1/a5y8f564xei", "/v1/irk7mbxr05p",
                "/opop/g48sx5lk9th", "/no/id", "/dummy/ok", "/x999/o9", "/mmm/xu3dugn8ubk", "/version/suffix/base_path", "/v1/fexs98lzrz3"
        );

        for (int i = 0; i < 1314520; i++) {
            for (String s : api) {
                try {
                    HttpUtils.sendGetData(String.format(uri, s, token), new HashMap<>());
                } catch (Exception e) {
                    //
                }
            }
        }
    }
    //@Test
    void callAsync() {
        String token = "eyJraWQiOiI5NGJhMDRkNC0wYWZjLTRmNzgtYjAyMi1kZTAwNGQ1ZTlmNmIiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiI1YzBlNzUwYjdhNWNkNDI0NjRhNTA5OWQiLCJjbHVzdGVyIjoiNjk2MGJkZmM5YjhhODM1MDU0OWFjY2NiIiwiY2xpZW50SWQiOiI1YzBlNzUwYjdhNWNkNDI0NjRhNTA5OWQiLCJyb2xlcyI6WyIkZXZlcnlvbmUiLCJhZG1pbiJdLCJpc3MiOiJodHRwOi8vMTI3LjAuMC4xOjMwMDAiLCJleHBpcmVkYXRlIjoxMTUzNDMyMzcwODgxOTgsImF1ZCI6IjVjMGU3NTBiN2E1Y2Q0MjQ2NGE1MDk5ZCIsImNyZWF0ZWRBdCI6MTc2ODcwOTA4ODE5OCwibmJmIjoxNzY4NzA5MDg4LCJleHAiOjExNTM0MzIzNzA4OCwiaWF0IjoxNzY4NzA5MDg4LCJqdGkiOiJkMjUzNWJhMS01NDQ0LTRiZjItYjRkNS0yNjk1YWFmNGVjNWIifQ.U7Jg-FQ9zdae-dinCuy86383raAN150sl77MDClJysaamET_ozXXPIK0EM9bAyOnQswYEeVlbx1h9usuB9r4V3ANRPjhdEocW1TUeQHQjXGaC0htwWVpw7yjXiMz0UPc56aNVBNeCLo9xVKK4-YntjuU5TBvh4oM_m-DTVwTkXUnz4P8CBIixW1FaRAiR6gvKu6k3o20qVRvBj9U9HX2z_wPSLpY2GHGJwpQ3A-SMCtFPqW5Sy5ULkQ5TiEHA6PEZ-8FGI67SOrlqOjPm_WKMU3kAtNCD51X2SVIMO9466H5kW1qHVk_pmVE67eIMJI3l05L4-Ar0OeGPCf79FHuJg";
        String uri = "http://127.0.0.1:3080/api/%s?access_token=%s";
        List<String> api = List.of("v1/tjq7duqpvs7", "v1/aslw80no7ze", "v1/tnuihy78hd1", "v1/c2hhm58iqvf",
                "/v1/we/y36xqmi0k0i", "/v1/sd/qq", "/v1/a7gei772p62", "/v1/tjq7duqpvs7", "/v1/aslw80no7ze", "/v1/tnuihy78hd1",
                "/v1/c2hhm58iqvf", "/v1/v1v1lerdf18", "/v1/zohp6j9z28a", "/v1/call/ekwoyltbqit", "/v1/call/fiyh6xusf8w", "/v1/a56jflpyrs8",
                "/v1/mbooiyue1w9", "/v1/po73y0ge6e7", "/v1/call/yt2cpjuhyfr", "/v1/call/fields", "/v1/jadevt6nzpm",
                "/v1/yw0n3lvjiku", "/v1/aqs919theqr", "/v1/agvegbzt3qx", "/v1/qrknw3gxn5c", "/v1/a5y8f564xei", "/v1/irk7mbxr05p",
                "/opop/g48sx5lk9th", "/no/id", "/dummy/ok", "/x999/o9", "/mmm/xu3dugn8ubk", "/version/suffix/base_path", "/v1/fexs98lzrz3"
        );

        for (int i = 0; i < 1314520; i++) {
            List<CompletableFuture<Void>> fs = new ArrayList<>(api.size());
            for (String s : api) {
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        HttpUtils.sendGetData(String.format(uri, s, token), new HashMap<>());
                    } catch (Exception e) {
                        //
                    }
                });
                fs.add(future);
            }
            try {
                fs.forEach(e -> {
                    try {
                        e.get();
                    } catch (Exception ex) {
                        //
                    }
                });
            } catch (Exception e) {

            }
        }
    }
}