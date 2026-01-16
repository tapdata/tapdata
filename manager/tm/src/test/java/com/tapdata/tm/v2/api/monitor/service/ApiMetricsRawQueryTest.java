package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.cluster.repository.ClusterStateRepository;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.modules.constant.ModuleStatusEnum;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.v2.api.monitor.main.dto.ApiDetail;
import com.tapdata.tm.v2.api.monitor.main.dto.ApiItem;
import com.tapdata.tm.v2.api.monitor.main.dto.ApiOfEachServer;
import com.tapdata.tm.v2.api.monitor.main.dto.ApiTopOnHomepage;
import com.tapdata.tm.v2.api.monitor.main.dto.ChartAndDelayOfApi;
import com.tapdata.tm.v2.api.monitor.main.dto.ServerChart;
import com.tapdata.tm.v2.api.monitor.main.dto.ServerItem;
import com.tapdata.tm.v2.api.monitor.main.dto.ServerOverviewDetail;
import com.tapdata.tm.v2.api.monitor.main.dto.ServerTopOnHomepage;
import com.tapdata.tm.v2.api.monitor.main.dto.TopApiInServer;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.param.ApiChart;
import com.tapdata.tm.v2.api.monitor.main.param.ApiDetailParam;
import com.tapdata.tm.v2.api.monitor.main.param.ApiListParam;
import com.tapdata.tm.v2.api.monitor.main.param.ApiWithServerDetail;
import com.tapdata.tm.v2.api.monitor.main.param.QueryBase;
import com.tapdata.tm.v2.api.monitor.main.param.ServerChartParam;
import com.tapdata.tm.v2.api.monitor.main.param.ServerDetail;
import com.tapdata.tm.v2.api.monitor.main.param.ServerListParam;
import com.tapdata.tm.v2.api.monitor.main.param.TopApiInServerParam;
import com.tapdata.tm.v2.api.monitor.main.param.TopWorkerInServerParam;
import com.tapdata.tm.utils.ApiMetricsDelayUtil;
import com.tapdata.tm.v2.api.usage.repository.ServerUsageMetricRepository;
import com.tapdata.tm.v2.api.usage.repository.UsageRepository;
import com.tapdata.tm.worker.dto.ApiServerStatus;
import com.tapdata.tm.worker.dto.ApiServerWorkerInfo;
import com.tapdata.tm.worker.dto.MetricInfo;
import com.tapdata.tm.worker.entity.ServerUsage;
import com.tapdata.tm.worker.entity.ServerUsageMetric;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongConsumer;

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

class ApiMetricsRawQueryTest {
    private ApiMetricsRawService service;
    private UsageRepository usageRepository;
    private WorkerRepository workerRepository;
    private ClusterStateRepository clusterRepository;
    private ModulesService modulesService;
    private MongoTemplate mongoTemplate;
    private ServerUsageMetricRepository serverUsageMetricRepository;
    private ApiMetricsRawQuery apiMetricsRawQuery;

    @BeforeEach
    void setUp() {
        service = mock(ApiMetricsRawService.class);
        usageRepository = mock(UsageRepository.class);
        workerRepository = mock(WorkerRepository.class);
        clusterRepository = mock(ClusterStateRepository.class);
        modulesService = mock(ModulesService.class);
        serverUsageMetricRepository = mock(ServerUsageMetricRepository.class);

        apiMetricsRawQuery = mock(ApiMetricsRawQuery.class);
        ReflectionTestUtils.setField(apiMetricsRawQuery, "service", service);
        ReflectionTestUtils.setField(apiMetricsRawQuery, "usageRepository", usageRepository);
        ReflectionTestUtils.setField(apiMetricsRawQuery, "workerRepository", workerRepository);
        ReflectionTestUtils.setField(apiMetricsRawQuery, "clusterRepository", clusterRepository);
        ReflectionTestUtils.setField(apiMetricsRawQuery, "modulesService", modulesService);
        ReflectionTestUtils.setField(apiMetricsRawQuery, "serverUsageMetricRepository", serverUsageMetricRepository);
        when(apiMetricsRawQuery.serverTopOnHomepage(any(QueryBase.class))).thenCallRealMethod();
        when(apiMetricsRawQuery.errorCount(anyList(), any(Function.class))).thenCallRealMethod();
        when(apiMetricsRawQuery.serverOverviewList(any(ServerListParam.class))).thenCallRealMethod();
        when(apiMetricsRawQuery.queryCpuUsageRecords(any(Criteria.class), anyLong(), anyLong(), anyInt())).thenCallRealMethod();
        when(apiMetricsRawQuery.mapUsage(anyList(), anyLong(), anyLong(), anyInt())).thenCallRealMethod();
        doCallRealMethod().when(apiMetricsRawQuery).asServerItemInfo(anyString(), any(ServerItem.class), anyMap(), any(Worker.class), anyMap(), any(ServerListParam.class));
        when(apiMetricsRawQuery.findServerById(anyString())).thenCallRealMethod();
        when(apiMetricsRawQuery.serverOverviewDetail(any(ServerDetail.class))).thenCallRealMethod();
        when(apiMetricsRawQuery.serverChart(any(ServerChartParam.class))).thenCallRealMethod();
        when(apiMetricsRawQuery.errorCountGetter(anyList(), any(LongConsumer.class))).thenCallRealMethod();
        when(apiMetricsRawQuery.topApiInServer(any(TopApiInServerParam.class))).thenCallRealMethod();
        when(apiMetricsRawQuery.topWorkerInServer(any(TopWorkerInServerParam.class))).thenCallRealMethod();
        when(apiMetricsRawQuery.apiTopOnHomepage(any(QueryBase.class))).thenCallRealMethod();
        when(apiMetricsRawQuery.apiOverviewList(any(ApiListParam.class))).thenCallRealMethod();
        when(apiMetricsRawQuery.apiOverviewDetail(any(ApiDetailParam.class))).thenCallRealMethod();
        when(apiMetricsRawQuery.findRowByApiId(any(Criteria.class), anyString(), any(QueryBase.class))).thenCallRealMethod();
        when(apiMetricsRawQuery.apiOfEachServer(any(ApiWithServerDetail.class))).thenCallRealMethod();
        when(apiMetricsRawQuery.delayOfApi(any(ApiChart.class))).thenCallRealMethod();
        when(apiMetricsRawQuery.mergeDelay(anyList())).thenCallRealMethod();
        when(apiMetricsRawQuery.publishApis()).thenCallRealMethod();
        when(apiMetricsRawQuery.extractIndex(anyString())).thenCallRealMethod();
        List<ModulesDto> modulesDtoLit = new ArrayList<>();
        ModulesDto m = new ModulesDto();
        m.setId(new ObjectId());
        modulesDtoLit.add(m);
        when(modulesService.findAllActiveApi(ModuleStatusEnum.ACTIVE)).thenReturn(modulesDtoLit);
        when(apiMetricsRawQuery.activeWorkers(anyList())).thenCallRealMethod();
        when(apiMetricsRawQuery.activeWorkers(anySet())).thenCallRealMethod();
        when(apiMetricsRawQuery.activeWorkers(null)).thenCallRealMethod();
        List<Worker> workers = new ArrayList<>();
        Worker e = new Worker();
        e.setProcessId("xxxxx");
        workers.add(e);
        when(workerRepository.findAll(any(Query.class))).thenReturn(workers);

    }

    @Nested
    class ServerTopOnHomepageTest {
        @Test
        void testEmptyApiMetricsRaws() {
            QueryBase param = new QueryBase();
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any(), any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(Collections.emptyList());
                when(service.find(any(Query.class))).thenReturn(Collections.emptyList());

                ServerTopOnHomepage result = apiMetricsRawQuery.serverTopOnHomepage(param);

                assertNotNull(result);
                assertEquals(0L, result.getTotalRequestCount());
            }
        }

        @Test
        void testWithApiMetricsRaws() {
            QueryBase param = new QueryBase();
            ApiMetricsRaw raw1 = createApiMetricsRaw("api1", "server1", 100L, 10L);
            ApiMetricsRaw raw2 = createApiMetricsRaw("api2", "server2", 200L, 20L);
            List<ApiMetricsRaw> raws = Arrays.asList(raw1, raw2);

            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class)) {

                analyzer.when(() -> ParticleSizeAnalyzer.of(any(), any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(raws);
                when(service.find(any(Query.class))).thenReturn(raws);

                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).mergeDelay(any());
                delayUtil.when(() -> ApiMetricsDelayUtil.sum(any())).thenReturn(1000L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(any(), anyLong())).thenReturn(950L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(any(), anyLong())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> {
                    return null;
                });

                ServerTopOnHomepage result = apiMetricsRawQuery.serverTopOnHomepage(param);

                assertNotNull(result);
                assertEquals(300L, result.getTotalRequestCount());
                assertEquals(30L, result.getErrorCount());
            }
        }

        @Test
        void testWithNullApiMetricsRaw() {
            QueryBase param = new QueryBase();
            List<ApiMetricsRaw> raws = Arrays.asList(null, createApiMetricsRaw("api1", "server1", 100L, 10L));

            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any(), any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(raws);
                when(service.find(any(Query.class))).thenReturn(raws);

                ServerTopOnHomepage result = apiMetricsRawQuery.serverTopOnHomepage(param);

                assertNotNull(result);
                assertEquals(100L, result.getTotalRequestCount());
            }
        }
    }

    @Nested
    class ErrorCountTest {
        @Test
        void testErrorCount() {
            ApiMetricsRaw raw1 = createApiMetricsRaw("api1", "server1", 100L, 10L);
            ApiMetricsRaw raw2 = createApiMetricsRaw("api1", "server1", 50L, 0L);
            ApiMetricsRaw raw3 = createApiMetricsRaw("api2", "server2", 200L, 20L);
            List<ApiMetricsRaw> raws = Arrays.asList(raw1, raw2, raw3);

            long result = apiMetricsRawQuery.errorCount(raws, ApiMetricsRaw::getApiId);

            assertEquals(2L, result); // api1 and api2 both have errors
        }

        @Test
        void testErrorCountNoErrors() {
            ApiMetricsRaw raw1 = createApiMetricsRaw("api1", "server1", 100L, 0L);
            ApiMetricsRaw raw2 = createApiMetricsRaw("api2", "server2", 200L, 0L);
            List<ApiMetricsRaw> raws = Arrays.asList(raw1, raw2);

            long result = apiMetricsRawQuery.errorCount(raws, ApiMetricsRaw::getApiId);

            assertEquals(0L, result);
        }
    }

    @Nested
    class ServerOverviewListTest {
        @Test
        void testEmptyServerInfos() {
            ServerListParam param = new ServerListParam();
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                when(workerRepository.findAll(any(Query.class))).thenReturn(Collections.emptyList());

                List<ServerItem> result = apiMetricsRawQuery.serverOverviewList(param);

                assertTrue(result.isEmpty());
            }
        }

        @Test
        void testWithServerInfos() {
            ServerListParam param = new ServerListParam();
            param.setServerName("test-server");
            param.setStartAt(System.currentTimeMillis() / 1000L - 3600);
            param.setEndAt(System.currentTimeMillis() / 1000L);
            param.setGranularity(2);
            Worker worker = createWorker("server1", "hostname1");
            List<Worker> workers = Arrays.asList(worker);
            ApiMetricsRaw raw = createApiMetricsRaw("api1", "server1", 100L, 10L);
            List<ApiMetricsRaw> raws = Arrays.asList(raw);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(raws);
                when(workerRepository.findAll(any(Query.class))).thenReturn(workers);
                when(service.find(any(Query.class))).thenReturn(raws);
                when(clusterRepository.findAll(any(Query.class))).thenReturn(Collections.emptyList());
//                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).queryCpuUsageRecords(any(Criteria.class), anyLong(), anyLong(), anyInt());
//                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).mergeDelay(any());
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(any(), anyInt())).thenReturn(950L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(any(), anyInt())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> null);
                List<ServerItem> result = apiMetricsRawQuery.serverOverviewList(param);
                assertNotNull(result);
                assertFalse(result.isEmpty());
            }
        }

        @Test
        void testGranularity0() {
            ServerListParam param = new ServerListParam();
            param.setServerName("test-server");
            param.setStartAt(System.currentTimeMillis() / 1000L - 3600);
            param.setEndAt(System.currentTimeMillis() / 1000L);
            param.setGranularity(0);
            Worker worker = createWorker("server1", "hostname1");
            List<Worker> workers = Arrays.asList(worker);
            ApiMetricsRaw raw = createApiMetricsRaw("api1", "server1", 100L, 10L);
            List<ApiMetricsRaw> raws = Arrays.asList(raw);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(raws);
                when(workerRepository.findAll(any(Query.class))).thenReturn(workers);
                when(service.find(any(Query.class))).thenReturn(raws);
                when(clusterRepository.findAll(any(Query.class))).thenReturn(Collections.emptyList());
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(any(), anyInt())).thenReturn(950L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(any(), anyInt())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> null);
                List<ServerItem> result = apiMetricsRawQuery.serverOverviewList(param);
                assertNotNull(result);
                assertFalse(result.isEmpty());
            }
        }
    }

    @Nested
    class FindServerByIdTest {
        @Test
        void testFindServerById() {
            String serverId = "server1";
            Worker worker = createWorker(serverId, "hostname1");
            when(workerRepository.findOne(any(Query.class))).thenReturn(Optional.of(worker));

            Worker result = apiMetricsRawQuery.findServerById(serverId);

            assertNotNull(result);
            assertEquals(serverId, result.getProcessId());
        }

        @Test
        void testFindServerByIdNotFound() {
            String serverId = "server1";
            when(workerRepository.findOne(any(Query.class))).thenReturn(Optional.empty());

            assertThrows(BizException.class, () -> apiMetricsRawQuery.findServerById(serverId));
        }
    }

    @Nested
    class ServerOverviewDetailTest {
        @Test
        void testEmptyServerId() {
            ServerDetail param = new ServerDetail();
            param.setServerId("");

            assertThrows(BizException.class, () -> apiMetricsRawQuery.serverOverviewDetail(param));
        }

        @Test
        void testNullServerId() {
            ServerDetail param = new ServerDetail();
            param.setServerId(null);

            assertThrows(BizException.class, () -> apiMetricsRawQuery.serverOverviewDetail(param));
        }

        @Test
        void testValidServerId() {
            ServerDetail param = new ServerDetail();
            param.setServerId("server1");
            param.setStartAt(System.currentTimeMillis() - 3600000);
            param.setEndAt(System.currentTimeMillis());

            Worker worker = createWorker("server1", "hostname1");
            ApiServerStatus status = new ApiServerStatus();
            MetricInfo metricInfo = new MetricInfo();
            metricInfo.setCpuUsage(0.5);
            metricInfo.setHeapMemoryUsage(600L);
            metricInfo.setLastUpdateTime(System.currentTimeMillis());
            status.setMetricValues(metricInfo);
            worker.setWorkerStatus(status);

            ApiMetricsRaw raw = createApiMetricsRaw("api1", "server1", 100L, 10L);
            List<ApiMetricsRaw> raws = Arrays.asList(raw);

            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class)) {

                analyzer.when(() -> ParticleSizeAnalyzer.of(any(), any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(raws);
                doReturn(worker).when(apiMetricsRawQuery).findServerById(anyString());
                when(service.find(any(Query.class))).thenReturn(raws);
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).mergeDelay(any());
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(any(), anyLong())).thenReturn(950L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(any(), anyInt())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> null);

                ServerOverviewDetail result = apiMetricsRawQuery.serverOverviewDetail(param);

                assertNotNull(result);
                assertEquals("server1", result.getServerId());
                assertEquals("hostname1", result.getServerName());
                assertEquals(100L, result.getRequestCount());
            }
        }
    }

    @Nested
    class ServerChartTest {
        @Test
        void testEmptyServerId() {
            ServerChartParam param = new ServerChartParam();
            param.setServerId("");

            assertThrows(BizException.class, () -> apiMetricsRawQuery.serverChart(param));
        }

        @Test
        void testValidServerId() {
            ServerChartParam param = new ServerChartParam();
            param.setServerId("server1");
            param.setStartAt(System.currentTimeMillis() - 3600000);
            param.setEndAt(System.currentTimeMillis());
            param.setGranularity(2);

            ApiMetricsRaw raw = createApiMetricsRaw("api1", "server1", 100L, 10L);
            raw.setTimeStart(System.currentTimeMillis());
            List<ApiMetricsRaw> raws = Arrays.asList(raw);

            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class)) {

                analyzer.when(() -> ParticleSizeAnalyzer.of(any(), any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(raws);
                when(service.find(any(Query.class))).thenReturn(raws);
                delayUtil.when(() -> ApiMetricsDelayUtil.sum(any())).thenReturn(1000L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(any(), anyLong())).thenReturn(950L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(any(), anyLong())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> null);

                ServerChart result = apiMetricsRawQuery.serverChart(param);

                assertNotNull(result);
                assertNotNull(result.getUsage());
                assertNotNull(result.getRequest());
                assertNotNull(result.getDelay());
            }
        }
    }

    @Nested
    class ErrorCountGetterTest {
        @Test
        void testErrorCountGetter() {
            ApiMetricsRaw raw1 = createApiMetricsRaw("api1", "server1", 100L, 10L);
            ApiMetricsRaw raw2 = createApiMetricsRaw("api2", "server1", 200L, 20L);
            List<ApiMetricsRaw> raws = Arrays.asList(raw1, raw2);

            List<Long> requestCounts = new ArrayList<>();
            long errorCount = apiMetricsRawQuery.errorCountGetter(raws, requestCounts::add);

            assertEquals(30L, errorCount);
            assertEquals(2, requestCounts.size());
            assertTrue(requestCounts.contains(100L));
            assertTrue(requestCounts.contains(200L));
        }

        @Test
        void testErrorCountGetterWithNull() {
            ApiMetricsRaw raw = createApiMetricsRaw("api1", "server1", null, null);
            List<ApiMetricsRaw> raws = Arrays.asList(null, raw);

            List<Long> requestCounts = new ArrayList<>();
            long errorCount = apiMetricsRawQuery.errorCountGetter(raws, requestCounts::add);

            assertEquals(0L, errorCount);
            assertEquals(1, requestCounts.size());
            assertTrue(requestCounts.contains(0L));
        }
    }

    @Nested
    class TopApiInServerTest {
        @Test
        void testEmptyServerId() {
            TopApiInServerParam param = new TopApiInServerParam();
            param.setServerId("");

            assertThrows(BizException.class, () -> apiMetricsRawQuery.topApiInServer(param));
        }

        @Test
        void testValidServerId() {
            TopApiInServerParam param = new TopApiInServerParam();
            param.setServerId("server1");
            param.setStartAt(System.currentTimeMillis() - 3600000);
            param.setEndAt(System.currentTimeMillis());
            param.setGranularity(2);
            QueryBase.SortInfo order = new QueryBase.SortInfo();
            order.setOrder("requestCount ASC");
            param.setSortInfo(order);

            ApiMetricsRaw raw = createApiMetricsRaw("api1", "server1", 100L, 10L);
            List<ApiMetricsRaw> raws = Arrays.asList(raw);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class);
                 MockedStatic<MongoUtils> mongoUtils = mockStatic(MongoUtils.class)) {

                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(raws);
                when(service.find(any(Query.class))).thenReturn(raws);
                mongoUtils.when(() -> MongoUtils.toObjectId(anyString())).thenReturn(new ObjectId());
                when(modulesService.findAll(any(Query.class))).thenReturn(new ArrayList<>());
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).mergeDelay(any());
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(any(), anyInt())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.sum(any())).thenReturn(1000L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> null);

                List<TopApiInServer> result = apiMetricsRawQuery.topApiInServer(param);

                assertNotNull(result);
            }
        }

        @Test
        void testEmptyApiIds() {
            TopApiInServerParam param = new TopApiInServerParam();
            param.setServerId("server1");

            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<MongoUtils> mongoUtils = mockStatic(MongoUtils.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(Collections.emptyList());
                when(service.find(any(Query.class))).thenReturn(Collections.emptyList());
                List<TopApiInServer> result = apiMetricsRawQuery.topApiInServer(param);
                assertFalse(result.isEmpty());
            }
        }

        @Test
        void sortERROR_RATE() {
            List<ApiMetricsRaw> apiMetricsRaws = new ArrayList<>();
            ApiMetricsRaw a1 = new ApiMetricsRaw();
            a1.setApiId(new ObjectId().toHexString());
            ApiMetricsRaw a2 = new ApiMetricsRaw();
            a2.setApiId(new ObjectId().toHexString());
            apiMetricsRaws.add(a1);
            apiMetricsRaws.add(a2);
            when(service.find(any(Query.class))).thenReturn(apiMetricsRaws);
            TopApiInServerParam param = new TopApiInServerParam();
            QueryBase.SortInfo order = new QueryBase.SortInfo();
            order.setOrder("errorRate DESC");
            param.setSortInfo(order);
            param.setServerId(new ObjectId().toHexString());
            param.setStartAt(System.currentTimeMillis() / 1000L - 3600);
            param.setEndAt(System.currentTimeMillis() / 1000L);
            List<ModulesDto> apiDtoList = new ArrayList<>();
            ModulesDto m1 = new ModulesDto();
            m1.setId(new ObjectId());
            m1.setName("name1");
            ModulesDto m2 = new ModulesDto();
            m2.setId(new ObjectId());
            m2.setName("name2");
            apiDtoList.add(m1);
            apiDtoList.add(m2);
            when(modulesService.findAll(any(Query.class))).thenReturn(apiDtoList);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(apiMetricsRaws);
                List<TopApiInServer> result = apiMetricsRawQuery.topApiInServer(param);
                assertFalse(result.isEmpty());
            }
        }

        @Test
        void sortAvg() {
            List<ApiMetricsRaw> apiMetricsRaws = new ArrayList<>();
            ApiMetricsRaw a1 = new ApiMetricsRaw();
            a1.setApiId(new ObjectId().toHexString());
            ApiMetricsRaw a2 = new ApiMetricsRaw();
            a2.setApiId(new ObjectId().toHexString());
            apiMetricsRaws.add(a1);
            apiMetricsRaws.add(a2);
            when(service.find(any(Query.class))).thenReturn(apiMetricsRaws);
            TopApiInServerParam param = new TopApiInServerParam();
            QueryBase.SortInfo order = new QueryBase.SortInfo();
            order.setOrder("avg");
            param.setSortInfo(order);
            param.setServerId(new ObjectId().toHexString());
            param.setStartAt(System.currentTimeMillis() / 1000L - 3600);
            param.setEndAt(System.currentTimeMillis() / 1000L);
            List<ModulesDto> apiDtoList = new ArrayList<>();
            ModulesDto m1 = new ModulesDto();
            m1.setId(new ObjectId());
            m1.setName("name1");
            ModulesDto m2 = new ModulesDto();
            m2.setId(new ObjectId());
            m2.setName("name2");
            apiDtoList.add(m1);
            apiDtoList.add(m2);
            when(modulesService.findAll(any(Query.class))).thenReturn(apiDtoList);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(apiMetricsRaws);
                List<TopApiInServer> result = apiMetricsRawQuery.topApiInServer(param);
                assertFalse(result.isEmpty());
            }
        }

        @Test
        void sortP99() {
            List<ApiMetricsRaw> apiMetricsRaws = new ArrayList<>();
            ApiMetricsRaw a1 = new ApiMetricsRaw();
            a1.setApiId(new ObjectId().toHexString());
            ApiMetricsRaw a2 = new ApiMetricsRaw();
            a2.setApiId(new ObjectId().toHexString());
            apiMetricsRaws.add(a1);
            apiMetricsRaws.add(a2);
            when(service.find(any(Query.class))).thenReturn(apiMetricsRaws);
            TopApiInServerParam param = new TopApiInServerParam();
            QueryBase.SortInfo order = new QueryBase.SortInfo();
            order.setOrder("p99 DESC");
            param.setSortInfo(order);
            param.setServerId(new ObjectId().toHexString());
            param.setStartAt(System.currentTimeMillis() / 1000L - 3600);
            param.setEndAt(System.currentTimeMillis() / 1000L);
            List<ModulesDto> apiDtoList = new ArrayList<>();
            ModulesDto m1 = new ModulesDto();
            m1.setId(new ObjectId());
            m1.setName("name1");
            ModulesDto m2 = new ModulesDto();
            m2.setId(new ObjectId());
            m2.setName("name2");
            apiDtoList.add(m1);
            apiDtoList.add(m2);
            when(modulesService.findAll(any(Query.class))).thenReturn(apiDtoList);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(apiMetricsRaws);
                List<TopApiInServer> result = apiMetricsRawQuery.topApiInServer(param);
                assertFalse(result.isEmpty());
            }
        }

        @Test
        void sortPDefault() {
            List<ApiMetricsRaw> apiMetricsRaws = new ArrayList<>();
            ApiMetricsRaw a1 = new ApiMetricsRaw();
            a1.setApiId(new ObjectId().toHexString());
            ApiMetricsRaw a2 = new ApiMetricsRaw();
            a2.setApiId(new ObjectId().toHexString());
            apiMetricsRaws.add(a1);
            apiMetricsRaws.add(a2);
            when(service.find(any(Query.class))).thenReturn(apiMetricsRaws);
            TopApiInServerParam param = new TopApiInServerParam();
            QueryBase.SortInfo order = new QueryBase.SortInfo();
            order.setOrder("");
            param.setSortInfo(order);
            param.setServerId(new ObjectId().toHexString());
            param.setStartAt(System.currentTimeMillis() / 1000L - 3600);
            param.setEndAt(System.currentTimeMillis() / 1000L);
            List<ModulesDto> apiDtoList = new ArrayList<>();
            ModulesDto m1 = new ModulesDto();
            m1.setId(new ObjectId());
            m1.setName("name1");
            ModulesDto m2 = new ModulesDto();
            m2.setId(new ObjectId());
            m2.setName("name2");
            apiDtoList.add(m1);
            apiDtoList.add(m2);
            when(modulesService.findAll(any(Query.class))).thenReturn(apiDtoList);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(apiMetricsRaws);
                List<TopApiInServer> result = apiMetricsRawQuery.topApiInServer(param);
                assertFalse(result.isEmpty());
            }
        }
    }

    @Nested
    class TopWorkerInServerTest {
        @Test
        void testEmptyServerId() {
            TopWorkerInServerParam param = new TopWorkerInServerParam();
            param.setServerId("");
            assertThrows(BizException.class, () -> apiMetricsRawQuery.topWorkerInServer(param));
        }

        @Test
        void testValidNotServerId() {
            TopWorkerInServerParam param = new TopWorkerInServerParam();
            param.setServerId("server1");
            param.setStartAt(System.currentTimeMillis() / 1000L - 3600);
            param.setEndAt(System.currentTimeMillis() / 1000L + 1L);
            param.setGranularity(2);

            WorkerCallEntity callEntity = new WorkerCallEntity();
            callEntity.setWorkOid("worker1");
            callEntity.setReqCount(100L);
            callEntity.setErrorCount(10L);
            ApiServerWorkerInfo workerInfo = new ApiServerWorkerInfo();
            workerInfo.setOid("worker1");
            workerInfo.setName("Worker-1");
            Worker w = new Worker();
            when(workerRepository.findOne(any(Query.class))).thenReturn(Optional.of(w));
            Assertions.assertDoesNotThrow(() -> apiMetricsRawQuery.topWorkerInServer(param));
        }

        @Test
        void testValidServerId() {
            TopWorkerInServerParam param = new TopWorkerInServerParam();
            param.setServerId("server1");
            param.setStartAt(System.currentTimeMillis() / 1000L - 3600);
            param.setEndAt(System.currentTimeMillis() / 1000L);
            param.setGranularity(2);

            WorkerCallEntity callEntity = new WorkerCallEntity();
            callEntity.setWorkOid("worker1");
            callEntity.setReqCount(100L);
            callEntity.setErrorCount(10L);
            List<WorkerCallEntity> callEntities = new ArrayList<>(Arrays.asList(callEntity));

            ApiServerWorkerInfo workerInfo = new ApiServerWorkerInfo();
            workerInfo.setOid("worker1");
            workerInfo.setName("Worker-1");
            Worker w = new Worker();
            w.setProcessId("processId");
            ApiServerStatus s = new ApiServerStatus();
            s.setWorkers(Map.of("worker1", workerInfo));
            w.setWorkerStatus(s);
            when(workerRepository.findOne(any(Query.class))).thenReturn(Optional.of(w));
            when(mongoTemplate.find(any(Query.class), any(Class.class), anyString())).thenReturn(callEntities);
            List<ServerUsageMetric> usage2 = new ArrayList<>();
            when(serverUsageMetricRepository.findAll(any(Query.class))).thenReturn(usage2);
            Assertions.assertDoesNotThrow(() -> apiMetricsRawQuery.topWorkerInServer(param));
        }

        @Test
        void testValidServerId1() {
            TopWorkerInServerParam param = new TopWorkerInServerParam();
            param.setServerId("server1");
            param.setStartAt(System.currentTimeMillis() / 1000L - 3600);
            param.setEndAt(System.currentTimeMillis() / 1000L);
            param.setGranularity(2);

            WorkerCallEntity callEntity = new WorkerCallEntity();
            callEntity.setWorkOid("worker1");
            callEntity.setReqCount(100L);
            callEntity.setErrorCount(10L);
            WorkerCallEntity callEntity2 = new WorkerCallEntity();
            callEntity2.setWorkOid("worker2");
            callEntity2.setReqCount(100L);
            callEntity2.setErrorCount(10L);
            List<WorkerCallEntity> callEntities = new ArrayList<>(Arrays.asList(callEntity, callEntity2));

            ApiServerWorkerInfo workerInfo = new ApiServerWorkerInfo();
            workerInfo.setOid("worker1");
            workerInfo.setName("Worker-1");
            ApiServerWorkerInfo workerInfo1 = new ApiServerWorkerInfo();
            workerInfo1.setOid("worker2");
            workerInfo1.setName("Worker-2");
            Worker w = new Worker();
            w.setProcessId("processId");
            ApiServerStatus s = new ApiServerStatus();
            s.setWorkers(Map.of("worker1", workerInfo, "worker2", workerInfo1));
            w.setWorkerStatus(s);
            when(workerRepository.findOne(any(Query.class))).thenReturn(Optional.of(w));
            when(mongoTemplate.find(any(Query.class), any(Class.class), anyString())).thenReturn(callEntities);
            List<ServerUsageMetric> usage2 = new ArrayList<>();
            when(serverUsageMetricRepository.findAll(any(Query.class))).thenReturn(usage2);
            Assertions.assertDoesNotThrow(() -> apiMetricsRawQuery.topWorkerInServer(param));
        }
    }

    @Nested
    class ApiTopOnHomepageTest {
        @Test
        void testEmptyApiMetricsRaws() {
            QueryBase param = new QueryBase();
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any(), any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(Collections.emptyList());
                when(service.find(any(Query.class))).thenReturn(Collections.emptyList());

                ApiTopOnHomepage result = apiMetricsRawQuery.apiTopOnHomepage(param);

                assertNotNull(result);
                assertEquals(0L, result.getApiCount());
            }
        }

        @Test
        void testWithApiMetricsRaws() {
            QueryBase param = new QueryBase();
            ApiMetricsRaw raw = createApiMetricsRaw("api1", "server1", 100L, 10L);
            List<ApiMetricsRaw> raws = Arrays.asList(raw);

            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class)) {

                analyzer.when(() -> ParticleSizeAnalyzer.of(any(), any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(raws);
                when(service.find(any(Query.class))).thenReturn(raws);
                delayUtil.when(() -> ApiMetricsDelayUtil.fixDelayAsMap(any())).thenReturn(new ArrayList<>());
                delayUtil.when(() -> ApiMetricsDelayUtil.sum(any())).thenReturn(1000L);

                ApiTopOnHomepage result = apiMetricsRawQuery.apiTopOnHomepage(param);

                assertNotNull(result);
                assertEquals(1L, result.getApiCount());
            }
        }
    }

    @Nested
    class ApiOverviewListTest {
        @Test
        void testEmptyApiMetricsRaws() {
            ApiListParam param = new ApiListParam();
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(Collections.emptyList());
                when(service.find(any(Query.class))).thenReturn(Collections.emptyList());

                List<ApiItem> result = apiMetricsRawQuery.apiOverviewList(param);

                assertFalse(result.isEmpty());
            }
        }

        @Test
        void testWithApiMetricsRaws() {
            ApiListParam param = new ApiListParam();
            QueryBase.SortInfo order = new QueryBase.SortInfo();
            order.setOrder("");
            param.setOrderBy(order);
            param.setGranularity(2);
            param.setStartAt(System.currentTimeMillis() - 3600000);
            param.setEndAt(System.currentTimeMillis());
            ApiMetricsRaw raw = createApiMetricsRaw("api1", "server1", 100L, 10L);
            List<ApiMetricsRaw> raws = new ArrayList<>(List.of(raw));

            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class);
                 MockedStatic<MongoUtils> mongoUtils = mockStatic(MongoUtils.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(raws);
                when(service.find(any(Query.class))).thenReturn(raws);
                mongoUtils.when(() -> MongoUtils.toObjectId(anyString())).thenReturn(new ObjectId());
                when(modulesService.findAll(any(Query.class))).thenReturn(new ArrayList<>());
                delayUtil.when(() -> ApiMetricsDelayUtil.fixDelayAsMap(any())).thenReturn(new ArrayList<>());
                delayUtil.when(() -> ApiMetricsDelayUtil.sum(anyList())).thenReturn(1000L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(anyList(), anyLong())).thenReturn(950L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(anyList(), anyLong())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> null);
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).mergeDelay(any());
                List<ApiItem> result = apiMetricsRawQuery.apiOverviewList(param);
                assertNotNull(result);
            }
        }

        @Test
        void testOrderByRequestCostAvg() {
            ApiListParam param = new ApiListParam();
            QueryBase.SortInfo order = new QueryBase.SortInfo();
            order.setOrder("requestCostAvg DESC");
            param.setOrderBy(order);
            param.setGranularity(2);
            param.setStartAt(System.currentTimeMillis() / 1000L - 3600);
            param.setEndAt(System.currentTimeMillis() / 1000L);
            ApiMetricsRaw raw = createApiMetricsRaw("api1", "server1", 100L, 10L);
            List<ApiMetricsRaw> raws = new ArrayList<>(List.of(raw));

            List<ModulesDto> allApi = new ArrayList<>();
            ModulesDto m1 = new ModulesDto();
            m1.setId(new ObjectId());
            m1.setName("name1");
            m1.setApiVersion("v1");
            m1.setBasePath("/api1");
            m1.setPrefix("prefix1");
            allApi.add(m1);
            ModulesDto m2 = new ModulesDto();
            m2.setId(m1.getId());
            m2.setName("name1");
            allApi.add(m2);
            when(modulesService.findAll(any(Query.class))).thenReturn(allApi);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class);
                 MockedStatic<MongoUtils> mongoUtils = mockStatic(MongoUtils.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(raws);
                when(service.find(any(Query.class))).thenReturn(raws);
                mongoUtils.when(() -> MongoUtils.toObjectId(anyString())).thenReturn(new ObjectId());

                delayUtil.when(() -> ApiMetricsDelayUtil.fixDelayAsMap(any())).thenReturn(new ArrayList<>());
                delayUtil.when(() -> ApiMetricsDelayUtil.sum(anyList())).thenReturn(1000L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(anyList(), anyLong())).thenReturn(950L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(anyList(), anyLong())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> null);
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).mergeDelay(any());
                List<ApiItem> result = apiMetricsRawQuery.apiOverviewList(param);
                assertNotNull(result);
            }
        }

        @Test
        void testOrderByP95() {
            ApiListParam param = new ApiListParam();
            QueryBase.SortInfo order = new QueryBase.SortInfo();
            order.setOrder("p95 DESC");
            param.setOrderBy(order);
            param.setGranularity(2);
            param.setStartAt(System.currentTimeMillis() / 1000L - 3600);
            param.setEndAt(System.currentTimeMillis() / 1000L);
            ObjectId apiId1 = new ObjectId();
            ObjectId apiId2 = new ObjectId();
            ApiMetricsRaw raw = createApiMetricsRaw(apiId1.toHexString(), "server", 100L, 10L);
            ApiMetricsRaw raw1 = createApiMetricsRaw(apiId2.toHexString(), "server", 200L, 20L);
            List<ApiMetricsRaw> raws = new ArrayList<>(List.of(raw, raw1));

            List<ModulesDto> allApi = new ArrayList<>();
            ModulesDto m1 = new ModulesDto();
            m1.setId(apiId1);
            m1.setName("api1");
            m1.setApiVersion("v1");
            m1.setBasePath("/api1");
            m1.setPrefix("prefix1");
            allApi.add(m1);
            ModulesDto m2 = new ModulesDto();
            m2.setId(apiId2);
            m2.setName("name1");
            m1.setApiVersion("v2");
            m1.setBasePath("/api2");
            m1.setPrefix("prefix2");
            allApi.add(m2);
            when(modulesService.findAll(any(Query.class))).thenReturn(allApi);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class);
                 MockedStatic<MongoUtils> mongoUtils = mockStatic(MongoUtils.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(raws);
                when(service.find(any(Query.class))).thenReturn(raws);
                mongoUtils.when(() -> MongoUtils.toObjectId(anyString())).thenReturn(new ObjectId());

                delayUtil.when(() -> ApiMetricsDelayUtil.fixDelayAsMap(any())).thenReturn(new ArrayList<>());
                delayUtil.when(() -> ApiMetricsDelayUtil.sum(anyList())).thenReturn(1000L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(anyList(), anyLong())).thenReturn(950L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(anyList(), anyLong())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> null);
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).mergeDelay(any());
                List<ApiItem> result = apiMetricsRawQuery.apiOverviewList(param);
                assertNotNull(result);
            }
        }

        @Test
        void testOrderByP99() {
            ApiListParam param = new ApiListParam();
            QueryBase.SortInfo order = new QueryBase.SortInfo();
            order.setOrder("p99 DESC");
            param.setOrderBy(order);
            param.setGranularity(2);
            param.setStartAt(System.currentTimeMillis() / 1000L - 3600);
            param.setEndAt(System.currentTimeMillis() / 1000L);
            ObjectId apiId1 = new ObjectId();
            ObjectId apiId2 = new ObjectId();
            ApiMetricsRaw raw = createApiMetricsRaw(apiId1.toHexString(), "server", 100L, 10L);
            ApiMetricsRaw raw1 = createApiMetricsRaw(apiId2.toHexString(), "server", 200L, 20L);
            List<ApiMetricsRaw> raws = new ArrayList<>(List.of(raw, raw1));

            List<ModulesDto> allApi = new ArrayList<>();
            ModulesDto m1 = new ModulesDto();
            m1.setId(apiId1);
            m1.setName("api1");
            m1.setApiVersion("v1");
            m1.setBasePath("/api1");
            m1.setPrefix("prefix1");
            allApi.add(m1);
            ModulesDto m2 = new ModulesDto();
            m2.setId(apiId2);
            m2.setName("name1");
            m1.setApiVersion("v2");
            m1.setBasePath("/api2");
            m1.setPrefix("prefix2");
            allApi.add(m2);
            when(modulesService.findAll(any(Query.class))).thenReturn(allApi);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class);
                 MockedStatic<MongoUtils> mongoUtils = mockStatic(MongoUtils.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(raws);
                when(service.find(any(Query.class))).thenReturn(raws);
                mongoUtils.when(() -> MongoUtils.toObjectId(anyString())).thenReturn(new ObjectId());

                delayUtil.when(() -> ApiMetricsDelayUtil.fixDelayAsMap(any())).thenReturn(new ArrayList<>());
                delayUtil.when(() -> ApiMetricsDelayUtil.sum(anyList())).thenReturn(1000L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(anyList(), anyLong())).thenReturn(950L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(anyList(), anyLong())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> null);
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).mergeDelay(any());
                List<ApiItem> result = apiMetricsRawQuery.apiOverviewList(param);
                assertNotNull(result);
            }
        }

        @Test
        void testOrderByERROR_RATE() {
            ApiListParam param = new ApiListParam();
            QueryBase.SortInfo order = new QueryBase.SortInfo();
            order.setOrder("errorRate DESC");
            param.setOrderBy(order);
            param.setGranularity(2);
            param.setStartAt(System.currentTimeMillis() / 1000L - 3600);
            param.setEndAt(System.currentTimeMillis() / 1000L);
            ObjectId apiId1 = new ObjectId();
            ObjectId apiId2 = new ObjectId();
            ApiMetricsRaw raw = createApiMetricsRaw(apiId1.toHexString(), "server", 100L, 10L);
            ApiMetricsRaw raw1 = createApiMetricsRaw(apiId2.toHexString(), "server", 200L, 20L);
            List<ApiMetricsRaw> raws = new ArrayList<>(List.of(raw, raw1));

            List<ModulesDto> allApi = new ArrayList<>();
            ModulesDto m1 = new ModulesDto();
            m1.setId(apiId1);
            m1.setName("api1");
            m1.setApiVersion("v1");
            m1.setBasePath("/api1");
            m1.setPrefix("prefix1");
            allApi.add(m1);
            ModulesDto m2 = new ModulesDto();
            m2.setId(apiId2);
            m2.setName("name1");
            m1.setApiVersion("v2");
            m1.setBasePath("/api2");
            m1.setPrefix("prefix2");
            allApi.add(m2);
            when(modulesService.findAll(any(Query.class))).thenReturn(allApi);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class);
                 MockedStatic<MongoUtils> mongoUtils = mockStatic(MongoUtils.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(raws);
                when(service.find(any(Query.class))).thenReturn(raws);
                mongoUtils.when(() -> MongoUtils.toObjectId(anyString())).thenReturn(new ObjectId());

                delayUtil.when(() -> ApiMetricsDelayUtil.fixDelayAsMap(any())).thenReturn(new ArrayList<>());
                delayUtil.when(() -> ApiMetricsDelayUtil.sum(anyList())).thenReturn(1000L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(anyList(), anyLong())).thenReturn(950L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(anyList(), anyLong())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> null);
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).mergeDelay(any());
                List<ApiItem> result = apiMetricsRawQuery.apiOverviewList(param);
                assertNotNull(result);
            }
        }

        @Test
        void testOrderByTotalRps() {
            ApiListParam param = new ApiListParam();
            QueryBase.SortInfo order = new QueryBase.SortInfo();
            order.setOrder("totalRps DESC");
            param.setOrderBy(order);
            param.setGranularity(2);
            param.setStartAt(System.currentTimeMillis() / 1000L - 3600);
            param.setEndAt(System.currentTimeMillis() / 1000L);
            ObjectId apiId1 = new ObjectId();
            ObjectId apiId2 = new ObjectId();
            ApiMetricsRaw raw = createApiMetricsRaw(apiId1.toHexString(), "server", 100L, 10L);
            ApiMetricsRaw raw1 = createApiMetricsRaw(apiId2.toHexString(), "server", 200L, 20L);
            List<ApiMetricsRaw> raws = new ArrayList<>(List.of(raw, raw1));

            List<ModulesDto> allApi = new ArrayList<>();
            ModulesDto m1 = new ModulesDto();
            m1.setId(apiId1);
            m1.setName("api1");
            m1.setApiVersion("v1");
            m1.setBasePath("/api1");
            m1.setPrefix("prefix1");
            allApi.add(m1);
            ModulesDto m2 = new ModulesDto();
            m2.setId(apiId2);
            m2.setName("name1");
            m1.setApiVersion("v2");
            m1.setBasePath("/api2");
            m1.setPrefix("prefix2");
            allApi.add(m2);
            when(modulesService.findAll(any(Query.class))).thenReturn(allApi);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class);
                 MockedStatic<MongoUtils> mongoUtils = mockStatic(MongoUtils.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(raws);
                when(service.find(any(Query.class))).thenReturn(raws);
                mongoUtils.when(() -> MongoUtils.toObjectId(anyString())).thenReturn(new ObjectId());

                delayUtil.when(() -> ApiMetricsDelayUtil.fixDelayAsMap(any())).thenReturn(new ArrayList<>());
                delayUtil.when(() -> ApiMetricsDelayUtil.sum(anyList())).thenReturn(1000L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(anyList(), anyLong())).thenReturn(950L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(anyList(), anyLong())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> null);
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).mergeDelay(any());
                List<ApiItem> result = apiMetricsRawQuery.apiOverviewList(param);
                assertNotNull(result);
            }
        }

        @Test
        void testApiIdsEmpty() {
            ApiListParam param = new ApiListParam();
            QueryBase.SortInfo order = new QueryBase.SortInfo();
            order.setOrder("totalRps DESC");
            param.setOrderBy(order);
            param.setGranularity(2);
            param.setStartAt(System.currentTimeMillis() / 1000L - 3600);
            param.setEndAt(System.currentTimeMillis() / 1000L);
            ObjectId apiId1 = new ObjectId();
            ObjectId apiId2 = new ObjectId();
            ApiMetricsRaw raw = createApiMetricsRaw("", "server", 100L, 10L);
            ApiMetricsRaw raw1 = createApiMetricsRaw("", "server", 200L, 20L);
            List<ApiMetricsRaw> raws = new ArrayList<>(List.of(raw, raw1));

            List<ModulesDto> allApi = new ArrayList<>();
            ModulesDto m1 = new ModulesDto();
            m1.setId(apiId1);
            m1.setName("api1");
            m1.setApiVersion("v1");
            m1.setBasePath("/api1");
            m1.setPrefix("prefix1");
            allApi.add(m1);
            ModulesDto m2 = new ModulesDto();
            m2.setId(apiId2);
            m2.setName("name1");
            m1.setApiVersion("v2");
            m1.setBasePath("/api2");
            m1.setPrefix("prefix2");
            allApi.add(m2);
            when(modulesService.findAll(any(Query.class))).thenReturn(allApi);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class);
                 MockedStatic<MongoUtils> mongoUtils = mockStatic(MongoUtils.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(raws);
                when(service.find(any(Query.class))).thenReturn(raws);
                mongoUtils.when(() -> MongoUtils.toObjectId(anyString())).thenReturn(new ObjectId());

                delayUtil.when(() -> ApiMetricsDelayUtil.fixDelayAsMap(any())).thenReturn(new ArrayList<>());
                delayUtil.when(() -> ApiMetricsDelayUtil.sum(anyList())).thenReturn(1000L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(anyList(), anyLong())).thenReturn(950L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(anyList(), anyLong())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> null);
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).mergeDelay(any());
                List<ApiItem> result = apiMetricsRawQuery.apiOverviewList(param);
                assertNotNull(result);
            }
        }
    }

    @Nested
    class ApiOverviewDetailTest {
        @Test
        void testWithApiMetricsRaws() {
            ApiDetailParam param = new ApiDetailParam();
            param.setApiId(new ObjectId().toHexString());
            ApiMetricsRaw raw = createApiMetricsRaw("api1", "server1", 100L, 10L);
            List<ApiMetricsRaw> raws = Arrays.asList(raw);
            ModulesDto apiInfo = new ModulesDto();
            apiInfo.setName("api1");
            apiInfo.setApiVersion("v1");
            apiInfo.setBasePath("/api1");
            apiInfo.setPrefix("prefix1");
            when(modulesService.findOne(any(Query.class))).thenReturn(apiInfo);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class)) {

                analyzer.when(() -> ParticleSizeAnalyzer.of(any(), any())).thenReturn(new Criteria());
                doReturn(raws).when(apiMetricsRawQuery).findRowByApiId(any(), anyString(), any());
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).mergeDelay(any());
                delayUtil.when(() -> ApiMetricsDelayUtil.fixDelayAsMap(any())).thenReturn(new ArrayList<>());
                delayUtil.when(() -> ApiMetricsDelayUtil.sum(any())).thenReturn(1000L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(any(), anyLong())).thenReturn(950L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(any(), anyLong())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> null);

                ApiDetail result = apiMetricsRawQuery.apiOverviewDetail(param);

                assertNotNull(result);
                assertEquals(100L, result.getRequestCount());
            }
        }

        @Test
        void testEmptyApiMetricsRaws() {
            ApiDetailParam param = new ApiDetailParam();
            param.setApiId(new ObjectId().toHexString());
            ModulesDto apiInfo = new ModulesDto();
            apiInfo.setName("api1");
            apiInfo.setApiVersion("");
            apiInfo.setBasePath("");
            apiInfo.setPrefix("");
            when(modulesService.findOne(any(Query.class))).thenReturn(apiInfo);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any(), any())).thenReturn(new Criteria());
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).findRowByApiId(any(), anyString(), any());

                ApiDetail result = apiMetricsRawQuery.apiOverviewDetail(param);

                assertNotNull(result);
            }
        }

        @Test
        void testEmptyApiMetricsRaws0() {
            ApiDetailParam param = new ApiDetailParam();
            param.setApiId("");
            ModulesDto apiInfo = new ModulesDto();
            apiInfo.setName("api1");
            apiInfo.setApiVersion("");
            apiInfo.setBasePath("");
            apiInfo.setPrefix("");
            when(modulesService.findOne(any(Query.class))).thenReturn(apiInfo);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any(), any())).thenReturn(new Criteria());
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).findRowByApiId(any(), anyString(), any());
                ApiDetail result = apiMetricsRawQuery.apiOverviewDetail(param);
                assertNotNull(result);
            }
        }
    }

    @Nested
    class FindRowByApiIdTest {
        @Test
        void testEmptyApiId() {
            Criteria criteria = new Criteria();
            QueryBase param = new QueryBase();

            assertThrows(BizException.class, () -> apiMetricsRawQuery.findRowByApiId(criteria, "", param));
        }

        @Test
        void testNullApiId() {
            Criteria criteria = new Criteria();
            QueryBase param = new QueryBase();
            when(apiMetricsRawQuery.findRowByApiId(criteria, null, param)).thenCallRealMethod();
            assertThrows(BizException.class, () -> apiMetricsRawQuery.findRowByApiId(criteria, null, param));
        }

        @Test
        void testValidApiId() {
            Criteria criteria = new Criteria();
            QueryBase param = new QueryBase();
            String apiId = "api1";
            List<ApiMetricsRaw> raws = Arrays.asList(createApiMetricsRaw(apiId, "server1", 100L, 10L));

            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(any(), any())).thenReturn(raws);
                when(service.find(any(Query.class))).thenReturn(raws);

                List<ApiMetricsRaw> result = apiMetricsRawQuery.findRowByApiId(criteria, apiId, param);

                assertNotNull(result);
                assertEquals(1, result.size());
            }
        }
    }

    @Nested
    class ApiOfEachServerTest {
        @Test
        void testEmptyApiMetricsRaws() {
            ApiWithServerDetail param = new ApiWithServerDetail();
            param.setApiId("api1");

            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).findRowByApiId(any(), anyString(), any());
                List<ApiOfEachServer> result = apiMetricsRawQuery.apiOfEachServer(param);

                assertFalse(result.isEmpty());
            }
        }

        @Test
        void testServerIdEmpty() {
            ApiWithServerDetail param = new ApiWithServerDetail();
            param.setApiId("api1");
            param.setOrderBy("");
            param.setStartAt(System.currentTimeMillis() - 3600000);
            param.setEndAt(System.currentTimeMillis());

            ApiMetricsRaw raw = createApiMetricsRaw("api1", "", 100L, 10L);
            ApiMetricsRaw raw2 = createApiMetricsRaw("api2", "", 100L, 10L);
            List<ApiMetricsRaw> raws = Arrays.asList(raw, raw2);
            Worker worker = createWorker("server1", "hostname1");
            List<Worker> workers = Arrays.asList(worker);
            when(service.find(any(Query.class))).thenReturn(raws);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(anyList(), any(QueryBase.class))).thenReturn(raws);
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                doReturn(raws).when(apiMetricsRawQuery).findRowByApiId(any(), anyString(), any());
                when(workerRepository.findAll(any(Query.class))).thenReturn(workers);
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).mergeDelay(any());
                delayUtil.when(() -> ApiMetricsDelayUtil.sum(any())).thenReturn(1000L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(any(), anyLong())).thenReturn(950L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(any(), anyLong())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> null);

                List<ApiOfEachServer> result = apiMetricsRawQuery.apiOfEachServer(param);

                assertNotNull(result);
            }
        }

        @Test
        void testServerIdAsSame() {
            ApiWithServerDetail param = new ApiWithServerDetail();
            param.setApiId("api1");
            param.setOrderBy("");
            param.setStartAt(System.currentTimeMillis() - 3600000);
            param.setEndAt(System.currentTimeMillis());

            ApiMetricsRaw raw = createApiMetricsRaw("api1", "s1", 100L, 10L);
            ApiMetricsRaw raw2 = createApiMetricsRaw("api1", "s1", 100L, 10L);
            List<ApiMetricsRaw> raws = Arrays.asList(raw, raw2);
            Worker worker = createWorker("server1", "hostname1");
            Worker worker1 = createWorker("server1", "hostname1");
            List<Worker> workers = Arrays.asList(worker, worker1);
            when(service.find(any(Query.class))).thenReturn(raws);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(anyList(), any(QueryBase.class))).thenReturn(raws);
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                doReturn(raws).when(apiMetricsRawQuery).findRowByApiId(any(), anyString(), any());
                when(workerRepository.findAll(any(Query.class))).thenReturn(workers);
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).mergeDelay(any());
                delayUtil.when(() -> ApiMetricsDelayUtil.sum(any())).thenReturn(1000L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(any(), anyLong())).thenReturn(950L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(any(), anyLong())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> null);

                List<ApiOfEachServer> result = apiMetricsRawQuery.apiOfEachServer(param);

                assertNotNull(result);
            }
        }

        @Test
        void testWithApiMetricsRaws() {
            ApiWithServerDetail param = new ApiWithServerDetail();
            param.setApiId("api1");
            param.setOrderBy("");
            param.setStartAt(System.currentTimeMillis() - 3600000);
            param.setEndAt(System.currentTimeMillis());

            ApiMetricsRaw raw = createApiMetricsRaw("api1", "server1", 100L, 10L);
            ApiMetricsRaw raw2 = createApiMetricsRaw("api2", "server2", 100L, 10L);
            List<ApiMetricsRaw> raws = Arrays.asList(raw, raw2);
            Worker worker = createWorker("server1", "hostname1");
            List<Worker> workers = Arrays.asList(worker);
            when(service.find(any(Query.class))).thenReturn(raws);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(anyList(), any(QueryBase.class))).thenReturn(raws);
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                doReturn(raws).when(apiMetricsRawQuery).findRowByApiId(any(), anyString(), any());
                when(workerRepository.findAll(any(Query.class))).thenReturn(workers);
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).mergeDelay(any());
                delayUtil.when(() -> ApiMetricsDelayUtil.sum(any())).thenReturn(1000L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(any(), anyLong())).thenReturn(950L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(any(), anyLong())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> null);

                List<ApiOfEachServer> result = apiMetricsRawQuery.apiOfEachServer(param);

                assertNotNull(result);
            }
        }

        @Test
        void testOrderByRequestCostAvg() {
            ApiWithServerDetail param = new ApiWithServerDetail();
            param.setApiId("api1");
            param.setOrderBy("requestCostAvg");
            param.setStartAt(System.currentTimeMillis() / 1000L - 3600);
            param.setEndAt(System.currentTimeMillis() / 1000L);

            ApiMetricsRaw raw = createApiMetricsRaw("api1", "server1", 0L, 10L);
            ApiMetricsRaw raw2 = createApiMetricsRaw("api2", "server2", 100L, 10L);
            List<ApiMetricsRaw> raws = Arrays.asList(raw, raw2);
            when(service.find(any(Query.class))).thenReturn(raws);
            Worker worker = createWorker("server1", "hostname1");
            List<Worker> workers = Arrays.asList(worker);

            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(anyList(), any(QueryBase.class))).thenReturn(raws);
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                doReturn(raws).when(apiMetricsRawQuery).findRowByApiId(any(), anyString(), any());
                when(workerRepository.findAll(any(Query.class))).thenReturn(workers);
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).mergeDelay(any());
                delayUtil.when(() -> ApiMetricsDelayUtil.sum(any())).thenReturn(1000L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(any(), anyLong())).thenReturn(950L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(any(), anyLong())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> null);

                List<ApiOfEachServer> result = apiMetricsRawQuery.apiOfEachServer(param);

                assertNotNull(result);
            }
        }

        @Test
        void testOrderByERROR_RATE() {
            ApiWithServerDetail param = new ApiWithServerDetail();
            param.setApiId("api1");
            param.setOrderBy("errorRate");
            param.setStartAt(System.currentTimeMillis() - 3600000);
            param.setEndAt(System.currentTimeMillis());

            ApiMetricsRaw raw = createApiMetricsRaw("api1", "server1", 100L, 10L);
            ApiMetricsRaw raw2 = createApiMetricsRaw("api2", "server2", 100L, 10L);
            List<ApiMetricsRaw> raws = Arrays.asList(raw, raw2);
            Worker worker = createWorker("server1", "hostname1");
            List<Worker> workers = Arrays.asList(worker);
            when(service.find(any(Query.class))).thenReturn(raws);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(anyList(), any(QueryBase.class))).thenReturn(raws);
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                doReturn(raws).when(apiMetricsRawQuery).findRowByApiId(any(), anyString(), any());
                when(workerRepository.findAll(any(Query.class))).thenReturn(workers);
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).mergeDelay(any());
                delayUtil.when(() -> ApiMetricsDelayUtil.sum(any())).thenReturn(1000L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(any(), anyLong())).thenReturn(950L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(any(), anyLong())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> null);

                List<ApiOfEachServer> result = apiMetricsRawQuery.apiOfEachServer(param);

                assertNotNull(result);
            }
        }

        @Test
        void testOrderByP95() {
            ApiWithServerDetail param = new ApiWithServerDetail();
            param.setApiId("api1");
            param.setOrderBy("p95");
            param.setStartAt(System.currentTimeMillis() - 3600000);
            param.setEndAt(System.currentTimeMillis());
            ApiMetricsRaw raw = createApiMetricsRaw("api1", "server1", 100L, 10L);
            ApiMetricsRaw raw2 = createApiMetricsRaw("api2", "server2", 100L, 10L);
            List<ApiMetricsRaw> raws = Arrays.asList(raw, raw2);
            Worker worker = createWorker("server1", "hostname1");
            List<Worker> workers = Arrays.asList(worker);
            when(service.find(any(Query.class))).thenReturn(raws);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(anyList(), any(QueryBase.class))).thenReturn(raws);
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                doReturn(raws).when(apiMetricsRawQuery).findRowByApiId(any(), anyString(), any());
                when(workerRepository.findAll(any(Query.class))).thenReturn(workers);
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).mergeDelay(any());
                delayUtil.when(() -> ApiMetricsDelayUtil.sum(any())).thenReturn(1000L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(any(), anyLong())).thenReturn(950L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(any(), anyLong())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> null);

                List<ApiOfEachServer> result = apiMetricsRawQuery.apiOfEachServer(param);

                assertNotNull(result);
            }
        }

        @Test
        void testOrderByP99() {
            ApiWithServerDetail param = new ApiWithServerDetail();
            param.setApiId("api1");
            param.setOrderBy("p99");
            param.setStartAt(System.currentTimeMillis() - 3600000);
            param.setEndAt(System.currentTimeMillis());
            ApiMetricsRaw raw = createApiMetricsRaw("api1", "server1", 100L, 10L);
            ApiMetricsRaw raw2 = createApiMetricsRaw("api2", "server2", 100L, 10L);
            List<ApiMetricsRaw> raws = Arrays.asList(raw, raw2);
            Worker worker = createWorker("server1", "hostname1");
            List<Worker> workers = Arrays.asList(worker);
            when(service.find(any(Query.class))).thenReturn(raws);
            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any())).thenReturn(new Criteria());
                analyzer.when(() -> ParticleSizeAnalyzer.apiMetricsRaws(anyList(), any(QueryBase.class))).thenReturn(raws);
                doReturn(raws).when(apiMetricsRawQuery).findRowByApiId(any(), anyString(), any());
                when(workerRepository.findAll(any(Query.class))).thenReturn(workers);
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).mergeDelay(any());
                delayUtil.when(() -> ApiMetricsDelayUtil.sum(any())).thenReturn(1000L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(any(), anyLong())).thenReturn(950L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(any(), anyLong())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> null);

                List<ApiOfEachServer> result = apiMetricsRawQuery.apiOfEachServer(param);

                assertNotNull(result);
            }
        }
    }

    @Nested
    class DelayOfApiTest {
        @Test
        void testEmptyApiMetricsRaws() {
            ApiChart param = new ApiChart();
            param.setApiId("api1");

            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class)) {
                analyzer.when(() -> ParticleSizeAnalyzer.of(any(), any())).thenReturn(new Criteria());
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).findRowByApiId(any(), anyString(), any());

                ChartAndDelayOfApi result = apiMetricsRawQuery.delayOfApi(param);

                assertNotNull(result);
            }
        }

        @Test
        void testWithApiMetricsRaws() {
            ApiChart param = new ApiChart();
            param.setApiId("api1");
            param.setStartAt(System.currentTimeMillis() - 3600000);
            param.setEndAt(System.currentTimeMillis());

            ApiMetricsRaw raw = createApiMetricsRaw("api1", "server1", 100L, 10L);
            raw.setTimeStart(System.currentTimeMillis());
            List<ApiMetricsRaw> raws = Arrays.asList(raw);

            try (MockedStatic<ParticleSizeAnalyzer> analyzer = mockStatic(ParticleSizeAnalyzer.class);
                 MockedStatic<ApiMetricsDelayUtil> delayUtil = mockStatic(ApiMetricsDelayUtil.class)) {

                analyzer.when(() -> ParticleSizeAnalyzer.of(any(), any())).thenReturn(new Criteria());
                doReturn(raws).when(apiMetricsRawQuery).findRowByApiId(any(), anyString(), any());
                doReturn(Collections.emptyList()).when(apiMetricsRawQuery).mergeDelay(any());
                delayUtil.when(() -> ApiMetricsDelayUtil.fixDelayAsMap(any())).thenReturn(new ArrayList<>());
                delayUtil.when(() -> ApiMetricsDelayUtil.sum(any())).thenReturn(1000L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p95(any(), anyLong())).thenReturn(950L);
                delayUtil.when(() -> ApiMetricsDelayUtil.p99(any(), anyLong())).thenReturn(990L);
                delayUtil.when(() -> ApiMetricsDelayUtil.readMaxAndMin(any(), any(), any())).thenAnswer(invocation -> null);

                ChartAndDelayOfApi result = apiMetricsRawQuery.delayOfApi(param);

                assertNotNull(result);
            }
        }
    }

    @Nested
    class MapUsageTest {
        @Test
        void testNormal() {
            List<? extends ServerUsage> infos = new ArrayList<>();
            long startAt = 1765209600L;
            long endAt = 1765209660L;
            int granularity = 0;
            ServerChart.Usage usage = apiMetricsRawQuery.mapUsage(infos, startAt, endAt, granularity);
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
            int granularity = 1;
            ServerChart.Usage usage = apiMetricsRawQuery.mapUsage(infos, startAt, endAt, granularity);
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
            int granularity = 2;
            ServerChart.Usage usage = apiMetricsRawQuery.mapUsage(infos, startAt, endAt, granularity);
            Assertions.assertNotNull(usage);
            Assertions.assertNotNull(usage.getCpuUsage());
            Assertions.assertNotNull(usage.getMemoryUsage());
            Assertions.assertNotNull(usage.getTs());
            Assertions.assertEquals(0, usage.getTs().size());
        }
        @Test
        void testMissingPreviousPart() {
            List<? extends ServerUsage> infos = new ArrayList<>();
            //2025-12-09 00:00:10
            ServerUsage instance1 = ServerUsageMetric.instance(1765209610000L, "processId", "workOid", 0);
            instance1.setCpuUsage(1.0D);
            instance1.setHeapMemoryUsage(100L);
            instance1.setHeapMemoryMax(100L);

            //2025-12-09 00:00:35
            ServerUsage instance2 = ServerUsageMetric.instance(1765209635000L, "processId", "workOid", 0);
            instance2.setCpuUsage(2.0D);
            instance2.setHeapMemoryUsage(200L);
            instance2.setHeapMemoryMax(400L);
            ((List<ServerUsage>) infos).add(instance1);
            ((List<ServerUsage>) infos).add(instance2);
            long startAt = 1765209600L; //2025-12-09 00:00:00
            long endAt   = 1765209660L; //2025-12-09 00:01:00
            int granularity = 0;
            ServerChart.Usage usage = apiMetricsRawQuery.mapUsage(infos, startAt, endAt, granularity);
            Assertions.assertNotNull(usage);
            Assertions.assertNotNull(usage.getCpuUsage());
            Assertions.assertNotNull(usage.getMemoryUsage());
            Assertions.assertNotNull(usage.getTs());
            Assertions.assertEquals(12, usage.getTs().size());
            long last = startAt;
            for (int i = 0; i < usage.getTs().size(); i++) {
                Long ts = usage.getTs().get(i);
                Assertions.assertEquals(last, ts);
                if (i == 2) {
                    Assertions.assertEquals(1.0D, usage.getCpuUsage().get(i));
                    Assertions.assertEquals(100.0D, usage.getMemoryUsage().get(i));
                } else if (i == 7) {
                    Assertions.assertEquals(2.0D, usage.getCpuUsage().get(i));
                    Assertions.assertEquals(50.0D, usage.getMemoryUsage().get(i));
                } else {
                    Assertions.assertNull(usage.getCpuUsage().get(i));
                    Assertions.assertNull(usage.getMemoryUsage().get(i));
                }
                last += 5;
            }
        }
    }

    @Nested
    class extractIndexTest {
        @Test
        void testNormal() {
            Assertions.assertEquals(0, apiMetricsRawQuery.extractIndex("Worker-0"));
            Assertions.assertEquals(1, apiMetricsRawQuery.extractIndex("Worker-1"));
            Assertions.assertEquals(1, apiMetricsRawQuery.extractIndex("Worker---1"));
            Assertions.assertEquals(Integer.MAX_VALUE, apiMetricsRawQuery.extractIndex("Worker1"));
            Assertions.assertEquals(Integer.MAX_VALUE, apiMetricsRawQuery.extractIndex("Worker---"));
            Assertions.assertEquals(Integer.MAX_VALUE, apiMetricsRawQuery.extractIndex(""));
            Assertions.assertEquals(Integer.MAX_VALUE, apiMetricsRawQuery.extractIndex("Worker-ddd"));
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
//
//    @Test
//    void call() {
//        String token
//= "eyJraWQiOiIxMTBmOGU3Mi1lZDUwLTQ3MzEtOTk5OC04YjBmMmI3NmVmMWEiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiI1YzBlNzUwYjdhNWNkNDI0NjRhNTA5OWQiLCJjbHVzdGVyIjoiNjk2MGJkZmM5YjhhODM1MDU0OWFjY2NiIiwiY2xpZW50SWQiOiI1YzBlNzUwYjdhNWNkNDI0NjRhNTA5OWQiLCJyb2xlcyI6WyIkZXZlcnlvbmUiLCJhZG1pbiJdLCJpc3MiOiJodHRwOi8vMTI3LjAuMC4xOjMwMDAiLCJleHBpcmVkYXRlIjoxNzY4NTM0MTQ3MzI4LCJhdWQiOiI1YzBlNzUwYjdhNWNkNDI0NjRhNTA5OWQiLCJjcmVhdGVkQXQiOjE3Njg1MzM4NDczMjgsIm5iZiI6MTc2ODUzMzg0NywiZXhwIjoxNzY4NTM0MTQ3LCJpYXQiOjE3Njg1MzM4NDcsImp0aSI6IjA0MGQwOTlkLTYxNzYtNDUyYy04ODBmLTAwYjU4OTNiZTBlMyJ9.SsLDpB1eepJEjnddUfiMdpF14vfrvfmYnw1mmWwPNy9azR1ibtWna4u3kbG34OoqulwaDj3vgXe1sTlSs9F3Q1Re2dcZvMmyp0QDonsBiL6q1g3g37t53TsD3xTG0bhInmMxRhpoLj9bS9WPmlTNz7RARAfrWh9tgDJvg21i6u22hgL7gmy8vsggaTtzs0tKp7EjV7M03g3bIOAXlm8atvq-YDyxy8Gv8n79ZII-QpYCWS1AS4aswKZik1iLDpPz7KzeyTXdVVaSaN9CzX2KGKzIyp5PVGxBbd5GXeT5KRxA7ThlzKpfI9Fqk4OxKlUu9dvGOfe_zuG6R9vpY0cEhA";
//        String uri = "http://127.0.0.1:3080/api/%s?access_token=%s";
//        List<String> api = List.of("v1/tjq7duqpvs7", "v1/aslw80no7ze", "v1/tnuihy78hd1", "v1/c2hhm58iqvf");
//        for (int i = 0; i < 10000; i++) {
//            for (String s : api) {
//                try {
//                    String string = HttpUtils.sendGetData(String.format(uri, s, token), new HashMap<>());
//                    Map map = JSON.parseObject(string, Map.class);
//                    if (map.get("error") instanceof Map<?,?> iMap && iMap.get("status") instanceof Number iNum && iNum.intValue() == 401) {
//                        token = token();
//                    }
//                    try {
//                        Thread.sleep(500);
//                    } catch (Exception e) {}
//                } catch (Exception e) {
//                    token = token();
//                    if (null == token) {
//                        return;
//                    }
//                }
//            }
//        }
//    }
//
//    String token() {
//        try {
//            Map<String, String> h = new HashMap<>();
//            h.put("Content-Type", "application/x-www-form-urlencoded");
//            String json = HttpUtils.sendGetData("http:127.0.0.1:3000/oauth/token?grant_type=client_credentials&client_id=5c0e750b7a5cd42464a5099d&client_secret={noop}eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9", h);
//            Map map = JSON.parseObject(json, Map.class);
//            return (String) map.get("access_token");
//        } catch (Exception e) {
//            return null;
//        }
//    }
}