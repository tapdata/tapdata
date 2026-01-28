package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.cluster.dto.Component;
import com.tapdata.tm.cluster.repository.ClusterStateRepository;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.modules.constant.ModuleStatusEnum;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.v2.api.monitor.main.dto.ApiDetail;
import com.tapdata.tm.v2.api.monitor.main.dto.ApiOfEachServer;
import com.tapdata.tm.v2.api.monitor.main.dto.ChartAndDelayOfApi;
import com.tapdata.tm.v2.api.monitor.main.dto.ServerChart;
import com.tapdata.tm.v2.api.monitor.main.dto.ServerItem;
import com.tapdata.tm.v2.api.monitor.main.dto.ServerOverviewDetail;
import com.tapdata.tm.v2.api.monitor.main.dto.ServerTopOnHomepage;
import com.tapdata.tm.v2.api.monitor.main.dto.TopWorkerInServer;
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
import com.tapdata.tm.v2.api.usage.repository.ServerUsageMetricRepository;
import com.tapdata.tm.v2.api.usage.repository.UsageRepository;
import com.tapdata.tm.worker.dto.ApiServerStatus;
import com.tapdata.tm.worker.dto.ApiServerWorkerInfo;
import com.tapdata.tm.worker.dto.MetricInfo;
import com.tapdata.tm.worker.entity.ServerUsage;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.repository.WorkerRepository;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ApiMetricsChartQueryTest {
    private ApiMetricsRawService service;
    private UsageRepository usageRepository;
    private WorkerRepository workerRepository;
    private ClusterStateRepository clusterRepository;
    private ModulesService modulesService;
    private ServerUsageMetricRepository serverUsageMetricRepository;
    private ApiMetricsRawMergeService metricsRawMergeService;
    private ApiMetricsChartQuery apiMetricsChartQuery;

    @BeforeEach
    void setUp() {
        service = mock(ApiMetricsRawService.class);
        usageRepository = mock(UsageRepository.class);
        workerRepository = mock(WorkerRepository.class);
        clusterRepository = mock(ClusterStateRepository.class);
        modulesService = mock(ModulesService.class);
        serverUsageMetricRepository = mock(ServerUsageMetricRepository.class);
        metricsRawMergeService = mock(ApiMetricsRawMergeService.class);

        apiMetricsChartQuery = mock(ApiMetricsChartQuery.class);
        ReflectionTestUtils.setField(apiMetricsChartQuery, "service", service);
        ReflectionTestUtils.setField(apiMetricsChartQuery, "usageRepository", usageRepository);
        ReflectionTestUtils.setField(apiMetricsChartQuery, "workerRepository", workerRepository);
        ReflectionTestUtils.setField(apiMetricsChartQuery, "clusterRepository", clusterRepository);
        ReflectionTestUtils.setField(apiMetricsChartQuery, "modulesService", modulesService);
        ReflectionTestUtils.setField(apiMetricsChartQuery, "serverUsageMetricRepository", serverUsageMetricRepository);
        ReflectionTestUtils.setField(apiMetricsChartQuery, "metricsRawMergeService", metricsRawMergeService);
        when(apiMetricsChartQuery.serverTopOnHomepage(any(QueryBase.class))).thenCallRealMethod();
        when(apiMetricsChartQuery.serverOverviewList(any(ServerListParam.class))).thenCallRealMethod();
        when(apiMetricsChartQuery.queryCpuUsageRecords(any(Criteria.class), anyLong(), anyLong(), any(TimeGranularity.class))).thenCallRealMethod();
        when(apiMetricsChartQuery.mapUsage(anyList(), anyLong(), anyLong(), any(TimeGranularity.class))).thenCallRealMethod();
        doCallRealMethod().when(apiMetricsChartQuery).asServerItemInfo(anyString(), any(ServerItem.class), anyMap(), any(Worker.class), anyMap(), any(ServerListParam.class));
        when(apiMetricsChartQuery.findServerById(anyString())).thenCallRealMethod();
        when(apiMetricsChartQuery.serverOverviewDetail(any(ServerDetail.class))).thenCallRealMethod();
        when(apiMetricsChartQuery.serverChart(any(ServerChartParam.class))).thenCallRealMethod();
        when(apiMetricsChartQuery.apiOverviewDetail(any(ApiDetailParam.class))).thenCallRealMethod();
        when(apiMetricsChartQuery.findRowByApiId(anyString(), any(QueryBase.class), any(MetricTypes.class), any(String[].class))).thenCallRealMethod();
        when(apiMetricsChartQuery.apiOfEachServer(any(ApiWithServerDetail.class))).thenCallRealMethod();
        when(apiMetricsChartQuery.delayOfApi(any(ApiChart.class))).thenCallRealMethod();
        when(apiMetricsChartQuery.extractIndex(anyString())).thenCallRealMethod();
        when(apiMetricsChartQuery.topWorkerInServer(anyList(), any(ServerDetail.class))).thenCallRealMethod();
        doCallRealMethod().when(apiMetricsChartQuery).handler(any(ServerChart.Item.class));
        doCallRealMethod().when(apiMetricsChartQuery).mapping(any(ChartAndDelayOfApi.Item.class));
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
        when(metricsRawMergeService.getDelay()).thenReturn(0L);
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

        @Test
        void testNullServerId() {
            ServerChartParam param = new ServerChartParam();
            param.setServerId(null);

            assertThrows(BizException.class, () -> apiMetricsChartQuery.serverChart(param));
        }

        @Test
        void testWithValidServerId() {
            ServerChartParam param = new ServerChartParam();
            param.setServerId("server1");
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            when(usageRepository.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            when(service.supplementMetricsRaw(any(), any(Boolean.class), any(), any(), any(String[].class))).thenReturn(new ArrayList<>());
            ServerChart result = apiMetricsChartQuery.serverChart(param);
            assertNotNull(result);
            assertNotNull(result.getRequest());
            assertNotNull(result.getDelay());
            assertNotNull(result.getDBCost());
        }

        @Test
        void testWithMinuteGranularity() {
            ServerChartParam param = new ServerChartParam();
            param.setServerId("server1");
            param.setGranularity(TimeGranularity.MINUTE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765213200L);
            when(serverUsageMetricRepository.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            when(service.supplementMetricsRaw(any(), any(Boolean.class), any(), any(), any(String[].class))).thenReturn(new ArrayList<>());
            ServerChart result = apiMetricsChartQuery.serverChart(param);
            assertNotNull(result);
        }

        @Test
        void testWithHourGranularity() {
            ServerChartParam param = new ServerChartParam();
            param.setServerId("server1");
            param.setGranularity(TimeGranularity.HOUR);
            param.setStartAt(1765209600L);
            param.setEndAt(1767801600L);
            when(serverUsageMetricRepository.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            when(service.supplementMetricsRaw(any(), any(Boolean.class), any(), any(), any(String[].class))).thenReturn(new ArrayList<>());
            ServerChart result = apiMetricsChartQuery.serverChart(param);
            assertNotNull(result);
        }
    }

    @Nested
    class FindRowByApiIdTest {

        @Test
        void testNullApiId() {
            QueryBase param = new QueryBase();
            when(apiMetricsChartQuery.findRowByApiId(null, param, MetricTypes.API, new String[]{})).thenCallRealMethod();
            assertThrows(BizException.class, () -> apiMetricsChartQuery.findRowByApiId(null, param, MetricTypes.API, new String[]{}));
        }

        @Test
        void testEmptyApiId() {
            QueryBase param = new QueryBase();
            //when(apiMetricsChartQuery.findRowByApiId("", param, MetricTypes.API, new String[]{})).thenCallRealMethod();
            assertThrows(BizException.class, () -> apiMetricsChartQuery.findRowByApiId("", param, MetricTypes.API, new String[]{}));
        }

        @Test
        void testValidApiId() {
            QueryBase param = new QueryBase();
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            when(metricsRawMergeService.merge(any(), any(), any(), any(String[].class))).thenReturn(new ArrayList<>());
            List<ApiMetricsRaw> result = apiMetricsChartQuery.findRowByApiId("testApiId", param, MetricTypes.API, new String[]{});
            assertNotNull(result);
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

        @Test
        void testNullName() {
            when(apiMetricsChartQuery.extractIndex(null)).thenCallRealMethod();
            Assertions.assertEquals(Integer.MAX_VALUE, apiMetricsChartQuery.extractIndex(null));
        }

        @Test
        void testBlankName() {
            Assertions.assertEquals(Integer.MAX_VALUE, apiMetricsChartQuery.extractIndex("   "));
        }

        @Test
        void testNameWithOnlyDash() {
            Assertions.assertEquals(Integer.MAX_VALUE, apiMetricsChartQuery.extractIndex("-"));
        }

        @Test
        void testNameStartingWithDash() {
            Assertions.assertEquals(Integer.MAX_VALUE, apiMetricsChartQuery.extractIndex("-0"));
        }
    }

    @Nested
    class ServerTopOnHomepageTest {
        @Test
        void testEmptyResult() {
            QueryBase param = new QueryBase();
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            when(metricsRawMergeService.merge(any(), any(), any(), any(String[].class))).thenReturn(new ArrayList<>());
            ServerTopOnHomepage result = apiMetricsChartQuery.serverTopOnHomepage(param);
            assertNotNull(result);
            assertEquals(0L, result.getTotalRequestCount());
        }

        @Test
        void testWithData() {
            QueryBase param = new QueryBase();
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            List<ApiMetricsRaw> raws = new ArrayList<>();
            raws.add(createApiMetricsRaw("api1", "server1", 100L, 10L));
            raws.add(createApiMetricsRaw("api2", "server1", 200L, 20L));
            when(metricsRawMergeService.merge(any(), any(), any(), any(String[].class))).thenReturn(raws);
            when(metricsRawMergeService.errorCount(any(), any(), any())).thenReturn(2L);
            doNothing().when(metricsRawMergeService).baseDataCalculate(any(), anyList(), any());
            ServerTopOnHomepage result = apiMetricsChartQuery.serverTopOnHomepage(param);
            assertNotNull(result);
            assertEquals(300L, result.getTotalRequestCount());
        }

        @Test
        void testWithNullElement() {
            QueryBase param = new QueryBase();
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            List<ApiMetricsRaw> raws = new ArrayList<>();
            raws.add(null);
            raws.add(createApiMetricsRaw("api1", "server1", 100L, 10L));
            when(metricsRawMergeService.merge(any(), any(), any(), any(String[].class))).thenReturn(raws);
            when(metricsRawMergeService.errorCount(any(), any(), any())).thenReturn(1L);
            doNothing().when(metricsRawMergeService).baseDataCalculate(any(), anyList(), any());
            ServerTopOnHomepage result = apiMetricsChartQuery.serverTopOnHomepage(param);
            assertNotNull(result);
            assertEquals(100L, result.getTotalRequestCount());
        }

        @Test
        void testWithZeroErrorCount() {
            QueryBase param = new QueryBase();
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            List<ApiMetricsRaw> raws = new ArrayList<>();
            raws.add(createApiMetricsRaw("api1", "server1", 100L, 0L));
            when(metricsRawMergeService.merge(any(), any(), any(), any(String[].class))).thenReturn(raws);
            doNothing().when(metricsRawMergeService).baseDataCalculate(any(), anyList(), any());
            ServerTopOnHomepage result = apiMetricsChartQuery.serverTopOnHomepage(param);
            assertNotNull(result);
            assertEquals(100L, result.getTotalRequestCount());
            assertEquals(0D, result.getTotalErrorRate());
        }
    }

    @Nested
    class ServerOverviewListTest {
        @Test
        void testEmptyServerList() {
            ServerListParam param = new ServerListParam();
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            when(workerRepository.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            List<ServerItem> result = apiMetricsChartQuery.serverOverviewList(param);
            assertNotNull(result);
        }

        @Test
        void testWithServerNameFilter() {
            ServerListParam param = new ServerListParam();
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            param.setServerName("test-server");
            List<Worker> workers = new ArrayList<>();
            Worker worker = createWorker("server1", "test-server-1");
            workers.add(worker);
            when(workerRepository.findAll(any(Query.class))).thenReturn(workers);
            when(clusterRepository.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            when(usageRepository.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            when(metricsRawMergeService.merge(any(), any(), any(), any(String[].class))).thenReturn(new ArrayList<>());
            List<ServerItem> result = apiMetricsChartQuery.serverOverviewList(param);
            assertNotNull(result);
        }
    }

    @Nested
    class QueryCpuUsageRecordsTest {
        @Test
        void testSecondFiveGranularity() {
            Criteria criteria = new Criteria();
            when(usageRepository.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            List<?> result = apiMetricsChartQuery.queryCpuUsageRecords(criteria, 1765209600L, 1765209660L, TimeGranularity.SECOND_FIVE);
            assertNotNull(result);
        }

        @Test
        void testMinuteGranularity() {
            Criteria criteria = new Criteria();
            when(serverUsageMetricRepository.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            List<?> result = apiMetricsChartQuery.queryCpuUsageRecords(criteria, 1765209600L, 1765213200L, TimeGranularity.MINUTE);
            assertNotNull(result);
        }

        @Test
        void testHourGranularity() {
            Criteria criteria = new Criteria();
            when(serverUsageMetricRepository.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            List<?> result = apiMetricsChartQuery.queryCpuUsageRecords(criteria, 1765209600L, 1767888000L, TimeGranularity.HOUR);
            assertNotNull(result);
        }
    }

    @Nested
    class AsServerItemInfoTest {
        @Test
        void testWithWorkerAndClusterState() {
            ServerItem item = ServerItem.create();
            Map<String, ServerChart.Usage> usageMap = new HashMap<>();
            ServerChart.Usage usage = ServerChart.Usage.create();
            usage.getCpuUsage().add(50.0);
            usage.getMemoryUsage().add(60.0);
            usage.getTs().add(1765209600L);
            usageMap.put("server1", usage);
            Worker worker = createWorker("server1", "test-host");
            ApiServerStatus status = new ApiServerStatus();
            status.setActiveTime(1765209600000L);
            status.setStatus("running");
            worker.setWorkerStatus(status);
            Map<String, Component> clusterStateMap = new HashMap<>();
            Component component = new Component();
            component.setStatus("running");
            clusterStateMap.put("server1", component);
            ServerListParam param = new ServerListParam();
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            apiMetricsChartQuery.asServerItemInfo("server1", item, usageMap, worker, clusterStateMap, param);
            assertEquals("server1", item.getServerId());
            assertEquals("test-host", item.getServerName());
            assertEquals("running", item.getServerStatus());
        }

        @Test
        void testWithNullWorker() {
            ServerItem item = ServerItem.create();
            Map<String, ServerChart.Usage> usageMap = new HashMap<>();
            Map<String, Component> clusterStateMap = new HashMap<>();
            ServerListParam param = new ServerListParam();
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            doCallRealMethod().when(apiMetricsChartQuery).asServerItemInfo("server1", item, usageMap, null, clusterStateMap, param);
            apiMetricsChartQuery.asServerItemInfo("server1", item, usageMap, null, clusterStateMap, param);
            assertEquals("server1", item.getServerId());
            assertEquals("", item.getServerName());
        }
    }

    @Nested
    class ServerOverviewDetailFullTest {
        @Test
        void testWithValidServerId() {
            ServerDetail param = new ServerDetail();
            param.setServerId("server1");
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            Worker worker = createWorker("server1", "test-host");
            ApiServerStatus status = new ApiServerStatus();
            MetricInfo metricInfo = new MetricInfo();
            metricInfo.setCpuUsage(50.0);
            metricInfo.setHeapMemoryUsage(1000L);
            metricInfo.setHeapMemoryUsageMax(2000L);
            metricInfo.setLastUpdateTime(1765209600000L);
            status.setMetricValues(metricInfo);
            worker.setWorkerStatus(status);
            when(workerRepository.findOne(any(Query.class))).thenReturn(Optional.of(worker));
            when(metricsRawMergeService.merge(any(), any(), any(), any(String[].class))).thenReturn(new ArrayList<>());
            when(metricsRawMergeService.errorCountGetter(anyList(), any())).thenReturn(0L);
            doNothing().when(metricsRawMergeService).baseDataCalculate(any(), anyList(), any());
            when(apiMetricsChartQuery.topWorkerInServer(anyList(), any(ServerDetail.class))).thenReturn(new TopWorkerInServer());
            ServerOverviewDetail result = apiMetricsChartQuery.serverOverviewDetail(param);
            assertNotNull(result);
            assertEquals("server1", result.getServerId());
            assertEquals("test-host", result.getServerName());
        }
    }

    @Nested
    class HandlerTest {
        @Test
        void testWithRequestCount() {
            ServerChart.Item item = new ServerChart.Item();
            item.setRequestCount(100L);
            item.setErrorCount(10L);
            item.setDelay(Arrays.asList(Map.of("100", 50)));
            item.setDbCost(Arrays.asList(Map.of("50", 30)));
            apiMetricsChartQuery.handler(item);
            assertNotNull(item.getErrorRate());
            assertEquals(10.0, item.getErrorRate());
        }

        @Test
        void testWithZeroRequestCount() {
            ServerChart.Item item = new ServerChart.Item();
            item.setRequestCount(0L);
            item.setErrorCount(0L);
            item.setDelay(new ArrayList<>());
            item.setDbCost(new ArrayList<>());
            apiMetricsChartQuery.handler(item);
            assertNull(item.getErrorRate());
        }

        @Test
        void testWithDelays() {
            ServerChart.Item item = new ServerChart.Item();
            item.setRequestCount(100L);
            item.setErrorCount(5L);
            item.setDelay(Arrays.asList(Map.of("100", 50)));
            item.setDbCost(Arrays.asList(Map.of("50", 30)));
            List<List<Map<String, Number>>> delays = new ArrayList<>();
            delays.add(Arrays.asList(Map.of("100", 50)));
            item.setDelays(delays);
            List<List<Map<String, Number>>> dbCosts = new ArrayList<>();
            dbCosts.add(Arrays.asList(Map.of("50", 30)));
            item.setDbCosts(dbCosts);
            apiMetricsChartQuery.handler(item);
            assertNull(item.getP95());
            assertNull(item.getP99());
        }

        @Test
        void testWithDbCostsOnlyNoDelays() {
            ServerChart.Item item = new ServerChart.Item();
            item.setRequestCount(100L);
            item.setErrorCount(5L);
            item.setDelay(Arrays.asList(Map.of("100", 50)));
            item.setDbCost(Arrays.asList(Map.of("50", 30)));
            item.setDelays(null);
            List<List<Map<String, Number>>> dbCosts = new ArrayList<>();
            dbCosts.add(Arrays.asList(Map.of("50", 30)));
            item.setDbCosts(dbCosts);
            apiMetricsChartQuery.handler(item);
            assertNull(item.getDbCostP95());
            assertNull(item.getDbCostP99());
        }

        @Test
        void testWithNullRequestCount() {
            ServerChart.Item item = new ServerChart.Item();
            item.setRequestCount(0l);
            item.setErrorCount(null);
            item.setDelay(new ArrayList<>());
            item.setDbCost(new ArrayList<>());
            apiMetricsChartQuery.handler(item);
            assertNull(item.getErrorRate());
        }
    }

    @Nested
    class TopWorkerInServerTest {
        @Test
        void testEmptyServerId() {
            ServerDetail param = new ServerDetail();
            param.setServerId("");
            assertThrows(BizException.class, () -> apiMetricsChartQuery.topWorkerInServer(new ArrayList<>(), param));
        }

        @Test
        void testNullServerId() {
            ServerDetail param = new ServerDetail();
            param.setServerId(null);
            assertThrows(BizException.class, () -> apiMetricsChartQuery.topWorkerInServer(new ArrayList<>(), param));
        }

        @Test
        void testWithWorkers() {
            ServerDetail param = new ServerDetail();
            param.setServerId("server1");
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setRealStart(1765209600L);
            param.setRealEnd(1765209660L);
            param.setEndAt(1765209660L);
            Worker worker = createWorker("server1", "test-host");
            ApiServerStatus status = new ApiServerStatus();
            Map<String, ApiServerWorkerInfo> workersMap = new HashMap<>();
            ApiServerWorkerInfo workerInfo = new ApiServerWorkerInfo();
            workerInfo.setOid("worker1");
            workerInfo.setName("Worker-0");
            MetricInfo metricInfo = new MetricInfo();
            metricInfo.setCpuUsage(50.0);
            workerInfo.setMetricValues(metricInfo);
            workersMap.put("worker1", workerInfo);
            status.setWorkers(workersMap);
            worker.setWorkerStatus(status);
            when(workerRepository.findOne(any(Query.class))).thenReturn(Optional.of(worker));
            when(usageRepository.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            TopWorkerInServer result = apiMetricsChartQuery.topWorkerInServer(new ArrayList<>(), param);
            assertNotNull(result);
            assertNotNull(result.getWorkerList());
        }

        @Test
        void testWithNullWorkerStatus() {
            ServerDetail param = new ServerDetail();
            param.setServerId("server1");
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            Worker worker = createWorker("server1", "test-host");
            worker.setWorkerStatus(null);
            when(workerRepository.findOne(any(Query.class))).thenReturn(Optional.of(worker));
            when(usageRepository.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            TopWorkerInServer result = apiMetricsChartQuery.topWorkerInServer(new ArrayList<>(), param);
            assertNotNull(result);
        }

        @Test
        void testWithEmptyWorkersMap() {
            ServerDetail param = new ServerDetail();
            param.setServerId("server1");
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            Worker worker = createWorker("server1", "test-host");
            ApiServerStatus status = new ApiServerStatus();
            status.setWorkers(new HashMap<>());
            worker.setWorkerStatus(status);
            when(workerRepository.findOne(any(Query.class))).thenReturn(Optional.of(worker));
            when(usageRepository.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            TopWorkerInServer result = apiMetricsChartQuery.topWorkerInServer(new ArrayList<>(), param);
            assertNotNull(result);
        }
    }

    @Nested
    class ApiOverviewDetailTest {
        @Test
        void testWithValidApiId() {
            ApiDetailParam param = new ApiDetailParam();
            param.setApiId(new ObjectId().toHexString());
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            List<ApiMetricsRaw> raws = new ArrayList<>();
            raws.add(createApiMetricsRaw(param.getApiId(), "server1", 100L, 10L));
            when(metricsRawMergeService.merge(any(), any(), any(), any(String[].class))).thenReturn(raws);
            ModulesDto modulesDto = new ModulesDto();
            modulesDto.setName("Test API");
            modulesDto.setApiVersion("v1");
            modulesDto.setBasePath("test");
            when(modulesService.findOne(any(Query.class))).thenReturn(modulesDto);
            doNothing().when(metricsRawMergeService).baseDataCalculate(any(), anyList(), any());
            ApiDetail result = apiMetricsChartQuery.apiOverviewDetail(param);
            assertNotNull(result);
            assertEquals("Test API", result.getApiName());
        }

        @Test
        void testWithEmptyApiMetrics() {
            ApiDetailParam param = new ApiDetailParam();
            param.setApiId("testApiId");
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            when(metricsRawMergeService.merge(any(), any(), any(), any(String[].class))).thenReturn(new ArrayList<>());
            ApiDetail result = apiMetricsChartQuery.apiOverviewDetail(param);
            assertNotNull(result);
        }

        @Test
        void testWithNullModulesDto() {
            ApiDetailParam param = new ApiDetailParam();
            param.setApiId(new ObjectId().toHexString());
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            List<ApiMetricsRaw> raws = new ArrayList<>();
            raws.add(createApiMetricsRaw(param.getApiId(), "server1", 100L, 10L));
            when(metricsRawMergeService.merge(any(), any(), any(), any(String[].class))).thenReturn(raws);
            when(modulesService.findOne(any(Query.class))).thenReturn(null);
            doNothing().when(metricsRawMergeService).baseDataCalculate(any(), anyList(), any());
            ApiDetail result = apiMetricsChartQuery.apiOverviewDetail(param);
            assertNotNull(result);
            assertEquals(param.getApiId(), result.getApiName());
        }

        @Test
        void testWithInvalidApiId() {
            ApiDetailParam param = new ApiDetailParam();
            param.setApiId("invalidApiId");
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            when(metricsRawMergeService.merge(any(), any(), any(), any(String[].class))).thenReturn(new ArrayList<>());
            ApiDetail result = apiMetricsChartQuery.apiOverviewDetail(param);
            assertNotNull(result);
            assertEquals("invalidApiId", result.getApiName());
        }
    }

    @Nested
    class ApiOfEachServerTest {
        @Test
        void testEmptyApiMetrics() {
            ApiWithServerDetail param = new ApiWithServerDetail();
            param.setApiId("testApiId");
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            when(metricsRawMergeService.merge(any(), any(), any(), any(String[].class))).thenReturn(new ArrayList<>());
            List<ApiOfEachServer> result = apiMetricsChartQuery.apiOfEachServer(param);
            assertNotNull(result);
        }

        @Test
        void testWithApiMetrics() {
            ApiWithServerDetail param = new ApiWithServerDetail();
            param.setApiId("testApiId");
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            List<ApiMetricsRaw> raws = new ArrayList<>();
            raws.add(createApiMetricsRaw("testApiId", "server1", 100L, 10L));
            when(metricsRawMergeService.merge(any(), any(), any(), any(String[].class))).thenReturn(raws);
            List<Worker> workers = new ArrayList<>();
            workers.add(createWorker("server1", "test-host"));
            when(workerRepository.findAll(any(Query.class))).thenReturn(workers);
            doNothing().when(metricsRawMergeService).baseDataCalculate(any(), anyList(), any());
            List<ApiOfEachServer> result = apiMetricsChartQuery.apiOfEachServer(param);
            assertNotNull(result);
        }

        @Test
        void testWithEmptyServerIds() {
            ApiWithServerDetail param = new ApiWithServerDetail();
            param.setApiId("testApiId");
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            List<ApiMetricsRaw> raws = new ArrayList<>();
            ApiMetricsRaw raw = createApiMetricsRaw("testApiId", null, 100L, 10L);
            raw.setProcessId(null);
            raws.add(raw);
            when(metricsRawMergeService.merge(any(), any(), any(), any(String[].class))).thenReturn(raws);
            List<ApiOfEachServer> result = apiMetricsChartQuery.apiOfEachServer(param);
            assertNotNull(result);
        }

        @Test
        void testWithBlankProcessId() {
            ApiWithServerDetail param = new ApiWithServerDetail();
            param.setApiId("testApiId");
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            List<ApiMetricsRaw> raws = new ArrayList<>();
            ApiMetricsRaw raw = createApiMetricsRaw("testApiId", "", 100L, 10L);
            raw.setProcessId("");
            raws.add(raw);
            when(metricsRawMergeService.merge(any(), any(), any(), any(String[].class))).thenReturn(raws);
            List<ApiOfEachServer> result = apiMetricsChartQuery.apiOfEachServer(param);
            assertNotNull(result);
        }
    }

    @Nested
    class DelayOfApiTest {
        @Test
        void testEmptyApiId() {
            ApiChart param = new ApiChart();
            param.setApiId("");
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            assertThrows(BizException.class, () -> apiMetricsChartQuery.delayOfApi(param));
        }

        @Test
        void testNullApiId() {
            ApiChart param = new ApiChart();
            param.setApiId(null);
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            assertThrows(BizException.class, () -> apiMetricsChartQuery.delayOfApi(param));
        }

        @Test
        void testEmptyApiMetrics() {
            ApiChart param = new ApiChart();
            param.setApiId("testApiId");
            param.setGranularity(TimeGranularity.SECOND_FIVE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765209660L);
            when(service.supplementMetricsRaw(any(), any(Boolean.class), any(), any(), any(String[].class))).thenReturn(new ArrayList<>());
            ChartAndDelayOfApi result = apiMetricsChartQuery.delayOfApi(param);
            assertNotNull(result);
        }

        @Test
        void testWithMinuteGranularity() {
            ApiChart param = new ApiChart();
            param.setApiId("testApiId");
            param.setGranularity(TimeGranularity.MINUTE);
            param.setStartAt(1765209600L);
            param.setEndAt(1765213200L);
            when(service.supplementMetricsRaw(any(), any(Boolean.class), any(), any(), any(String[].class))).thenReturn(new ArrayList<>());
            ChartAndDelayOfApi result = apiMetricsChartQuery.delayOfApi(param);
            assertNotNull(result);
        }

        @Test
        void testWithHourGranularity() {
            ApiChart param = new ApiChart();
            param.setApiId("testApiId");
            param.setGranularity(TimeGranularity.HOUR);
            param.setStartAt(1765209600L);
            param.setEndAt(1767801600L);
            when(service.supplementMetricsRaw(any(), any(Boolean.class), any(), any(), any(String[].class))).thenReturn(new ArrayList<>());
            ChartAndDelayOfApi result = apiMetricsChartQuery.delayOfApi(param);
            assertNotNull(result);
        }
    }

    @Nested
    class MappingTest {
        @Test
        void testWithRequestCount() {
            ChartAndDelayOfApi.Item item = new ChartAndDelayOfApi.Item();
            item.setDelay(Arrays.asList(Map.of("100", 50)));
            item.setDbCost(Arrays.asList(Map.of("50", 30)));
            item.setTotalBytes(1000L);
            apiMetricsChartQuery.mapping(item);
            assertNotNull(item.getRequestCostAvg());
            assertNotNull(item.getRps());
        }

        @Test
        void testWithZeroRequestCount() {
            ChartAndDelayOfApi.Item item = new ChartAndDelayOfApi.Item();
            item.setDelay(new ArrayList<>());
            item.setDbCost(new ArrayList<>());
            item.setTotalBytes(0L);
            apiMetricsChartQuery.mapping(item);
            assertEquals(0D, item.getRequestCostAvg());
            assertEquals(0D, item.getDbCostAvg());
        }

        @Test
        void testWithDelaysAndDbCosts() {
            ChartAndDelayOfApi.Item item = new ChartAndDelayOfApi.Item();
            item.setDelay(Arrays.asList(Map.of("100", 50)));
            item.setDbCost(Arrays.asList(Map.of("50", 30)));
            item.setTotalBytes(1000L);
            List<List<Map<String, Number>>> delays = new ArrayList<>();
            delays.add(Arrays.asList(Map.of("100", 50)));
            item.setDelays(delays);
            List<List<Map<String, Number>>> dbCosts = new ArrayList<>();
            dbCosts.add(Arrays.asList(Map.of("50", 30)));
            item.setDbCosts(dbCosts);
            apiMetricsChartQuery.mapping(item);
            assertNull(item.getP95());
            assertNull(item.getP99());
        }

        @Test
        void testWithDbCostsOnly() {
            ChartAndDelayOfApi.Item item = new ChartAndDelayOfApi.Item();
            item.setDelay(new ArrayList<>());
            item.setDbCost(Arrays.asList(Map.of("50", 30)));
            item.setTotalBytes(0L);
            List<List<Map<String, Number>>> dbCosts = new ArrayList<>();
            dbCosts.add(Arrays.asList(Map.of("50", 30)));
            item.setDbCosts(dbCosts);
            apiMetricsChartQuery.mapping(item);
            assertNull(item.getDbCostP95());
            assertNull(item.getDbCostP99());
        }
    }

    @Nested
    class ActiveWorkersTest {
        @Test
        void testWithNullIgnoreIds() {
            Map<String, Worker> result = apiMetricsChartQuery.activeWorkers(null);
            assertNotNull(result);
        }

        @Test
        void testWithEmptyIgnoreIds() {
            Map<String, Worker> result = apiMetricsChartQuery.activeWorkers(new ArrayList<>());
            assertNotNull(result);
        }

        @Test
        void testWithIgnoreIds() {
            Map<String, Worker> result = apiMetricsChartQuery.activeWorkers(Arrays.asList("server1", "server2"));
            assertNotNull(result);
        }

        @Test
        void testWithSetIgnoreIds() {
            Map<String, Worker> result = apiMetricsChartQuery.activeWorkers(new HashSet<>(Arrays.asList("server1", "server2")));
            assertNotNull(result);
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
}