package com.tapdata.tm.task.service.dashboard;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.cluster.dto.AccessNodeInfo;
import com.tapdata.tm.cluster.dto.ClusterStateDto;
import com.tapdata.tm.cluster.dto.Component;
import com.tapdata.tm.cluster.dto.SystemInfo;
import com.tapdata.tm.cluster.repository.ClusterStateRepository;
import com.tapdata.tm.cluster.service.ClusterStateService;
import com.tapdata.tm.commons.metrics.MetricCons;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.monitor.service.MeasurementServiceV2Impl;
import com.tapdata.tm.task.res.CpuMemoryService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.chart.ChartViewService;
import com.tapdata.tm.task.vo.TaskDashboardVo;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import com.tapdata.tm.v2.api.monitor.main.param.QueryBase;
import com.tapdata.tm.v2.api.monitor.repository.ApiMetricsRepository;
import com.tapdata.tm.v2.api.monitor.service.ApiMetricsChartQuery;
import com.tapdata.tm.v2.api.monitor.service.ApiMetricsRawMergeService;
import com.tapdata.tm.v2.api.monitor.service.ApiMetricsRawService;
import com.tapdata.tm.v2.api.pool.repository.ConnectionPoolInfoRepository;
import com.tapdata.tm.v2.api.usage.repository.ServerUsageMetricRepository;
import com.tapdata.tm.v2.api.usage.repository.UsageRepository;
import com.tapdata.tm.worker.repository.WorkerRepository;
import io.tapdata.common.sample.request.Sample;
import io.tapdata.common.sample.request.SampleRequest;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.query.Query;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 系统趋势真实装配验证（L2）。
 *
 * <p>使用最小 Spring 上下文装配真实的 {@link MeasurementServiceV2Impl}、{@link ApiMetricsRawService}、
 * {@link ApiMetricsChartQuery} 与 {@link TaskDashboardService}，只为外部协作者（任务、数据源、集群状态）
 * 提供 mock；在真实 Mongo 副本集上验证 measurement 与 API 查询链路、节点归属、无身份数据标记，
 * 以及节点未部署 API Server 时的组件状态。</p>
 */
class SystemTrendsAssemblyIT {

    private static final String NODE_A = "node-A";
    private static final String NODE_B = "node-B";
    private static final String PROCESS_A = "process-A";

    @Test
    void assemblesDashboardTrendsFromRealMongoWithNodeAttribution() throws Exception {
        String mongoUri = System.getenv("TAPDATA_TEST_MONGO_URI");
        Assumptions.assumeTrue(mongoUri != null && !mongoUri.isBlank(),
                "TAPDATA_TEST_MONGO_URI must point to an isolated Mongo replica set (transactions are required)");
        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getenv("TAPDATA_TEST_MONGO_ALLOW_TEMP_DATABASE")),
                "TAPDATA_TEST_MONGO_ALLOW_TEMP_DATABASE=true is required because the test creates and drops a random database");

        try (MongoClient client = MongoClients.create(mongoUri)) {
            Assumptions.assumeTrue(isReplicaSetOrMongos(client),
                    "TAPDATA_TEST_MONGO_URI must point to a replica set or mongos: the API trend query starts a SNAPSHOT transaction");

            String databaseName = "tap12023_trends_" + UUID.randomUUID().toString().replace("-", "");
            MongoTemplate mongoTemplate = new MongoTemplate(new SimpleMongoClientDatabaseFactory(client, databaseName));
            try {
                String taskId = new ObjectId().toHexString();
                long nowMillis = System.currentTimeMillis();
                long minuteStart = nowMillis - nowMillis % 60_000L - 60_000L;
                long sampleStart = minuteStart + 1_000L;

                AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
                try {
                    TaskDto task = new TaskDto();
                    task.setId(new ObjectId(taskId));
                    task.setStatus(TaskDto.STATUS_RUNNING);

                    ChartViewService chartViewService = mock(ChartViewService.class);
                    when(chartViewService.getViewTaskDtoByUser(any())).thenReturn(List.of(task));
                    DataSourceService dataSourceService = mock(DataSourceService.class);
                    when(dataSourceService.findAll(any(Query.class))).thenReturn(new ArrayList<>());
                    ClusterStateService clusterStateService = mock(ClusterStateService.class);
                    when(clusterStateService.findAll(any(Query.class))).thenReturn(List.of(clusterState()));
                    when(clusterStateService.findAccessNodeInfo(any(), anyCollection())).thenReturn(List.of(accessNode()));

                    context.getBeanFactory().registerSingleton("obsMongoTemplate", CompletableFuture.completedFuture(mongoTemplate));
                    context.getBeanFactory().registerSingleton("mongoTemplate", mongoTemplate);
                    context.getBeanFactory().registerSingleton("metadataInstancesService", mock(MetadataInstancesService.class));
                    context.getBeanFactory().registerSingleton("taskService", mock(TaskService.class));
                    context.getBeanFactory().registerSingleton("cpuMemoryService", mock(CpuMemoryService.class));
                    context.getBeanFactory().registerSingleton("apiMetricsRepository", mock(ApiMetricsRepository.class));
                    // ApiMetricsChartQuery collaborators are not used by homepageRequestTrendV2; beans keep the real wiring complete.
                    context.getBeanFactory().registerSingleton("usageRepository", mock(UsageRepository.class));
                    context.getBeanFactory().registerSingleton("workerRepository", mock(WorkerRepository.class));
                    context.getBeanFactory().registerSingleton("clusterRepository", mock(ClusterStateRepository.class));
                    context.getBeanFactory().registerSingleton("modulesService", mock(ModulesService.class));
                    context.getBeanFactory().registerSingleton("serverUsageMetricRepository", mock(ServerUsageMetricRepository.class));
                    context.getBeanFactory().registerSingleton("metricsRawMergeService", mock(ApiMetricsRawMergeService.class));
                    context.getBeanFactory().registerSingleton("poolInfoRepository", mock(ConnectionPoolInfoRepository.class));
                    context.getBeanFactory().registerSingleton("chartViewService", chartViewService);
                    context.getBeanFactory().registerSingleton("dataSourceService", dataSourceService);
                    context.getBeanFactory().registerSingleton("clusterStateService", clusterStateService);
                    context.getBeanFactory().registerSingleton("clock",
                            Clock.fixed(Instant.ofEpochMilli(minuteStart + 120_000L), ZoneOffset.UTC));
                    context.register(MeasurementServiceV2Impl.class, ApiMetricsRawService.class,
                            ApiMetricsChartQuery.class, TaskDashboardService.class);
                    context.refresh();

                    MeasurementServiceV2Impl measurementService = context.getBean(MeasurementServiceV2Impl.class);
                    measurementService.addAgentMeasurement(List.of(
                            sample(tags(taskId, NODE_A, "engine-A"), sampleStart + 100L, 10D),
                            sample(tags(taskId, NODE_A, "engine-A"), sampleStart + 200L, 10D)));
                    measurementService.addAgentMeasurement(List.of(
                            sample(tags(taskId, NODE_B, "engine-B"), sampleStart + 300L, 20D),
                            sample(tags(taskId, NODE_B, "engine-B"), sampleStart + 400L, 20D)));

                    long rangeEnd = minuteStart + 60_000L;
                    assertTrend(measurementService, taskId, minuteStart, rangeEnd, NODE_A, 5D);
                    assertTrend(measurementService, taskId, minuteStart, rangeEnd, NODE_B, 10D);
                    assertTrend(measurementService, taskId, minuteStart, rangeEnd, null, 15D);

                    long apiBucket = minuteStart - 3_600_000L;
                    long untraceableBucket = apiBucket + 60_000L;
                    mongoTemplate.save(raw(apiBucket, MetricTypes.SERER, NODE_A, 10L));
                    mongoTemplate.save(raw(apiBucket, MetricTypes.SERER, NODE_B, 20L));
                    mongoTemplate.save(raw(apiBucket, MetricTypes.SERER, null, 5L));
                    mongoTemplate.save(raw(apiBucket, MetricTypes.ALL, null, 35L));
                    mongoTemplate.save(raw(untraceableBucket, MetricTypes.SERER, null, 7L));

                    ApiMetricsChartQuery chartQuery = context.getBean(ApiMetricsChartQuery.class);
                    QueryBase nodeQuery = apiQuery(apiBucket / 1000L, (apiBucket + 60_000L) / 1000L, NODE_A);
                    assertEquals(10D / 60D, chartQuery.homepageRequestTrendV2(nodeQuery, NODE_A).getValues().get(0), 0.000001D);

                    QueryBase allQuery = apiQuery(apiBucket / 1000L, (apiBucket + 60_000L) / 1000L, null);
                    var allTrend = chartQuery.homepageRequestTrendV2(allQuery, null);
                    assertEquals(35D / 60D, allTrend.getValues().get(0), 0.000001D);
                    assertEquals("partial", allTrend.getTraceability());
                    assertTrue(allTrend.isHasUnattributedData());

                    QueryBase untraceableQuery = apiQuery(untraceableBucket / 1000L, (untraceableBucket + 60_000L) / 1000L, null);
                    var untraceableTrend = chartQuery.homepageRequestTrendV2(untraceableQuery, null);
                    assertEquals("untraceable", untraceableTrend.getTraceability());
                    assertEquals("no_data", untraceableTrend.getStatus());

                    TaskDashboardService dashboardService = context.getBean(TaskDashboardService.class);
                    UserDetail user = new UserDetail("user-a", "customer", "user-a", "password", List.of());
                    TaskDashboardVo nodeDashboard = dashboardService.dashboard(user, "range", 3600L, "trends", 5,
                            NODE_A, minuteStart / 1000L, rangeEnd / 1000L, 2);
                    assertEquals(minuteStart / 1000L, nodeDashboard.getTrends().getThroughput().getTs().get(0));
                    assertEquals(5D, nodeDashboard.getTrends().getThroughput().getValues().get(0), 0.000001D);
                    assertEquals("component_not_deployed", nodeDashboard.getTrends().getApiRequests().getStatus());

                    TaskDashboardVo allDashboard = dashboardService.dashboard(user, "range", 3600L, "trends", 5,
                            null, minuteStart / 1000L, rangeEnd / 1000L, 2);
                    assertEquals(minuteStart / 1000L, allDashboard.getTrends().getThroughput().getTs().get(0));
                    assertEquals(15D, allDashboard.getTrends().getThroughput().getValues().get(0), 0.000001D);
                } finally {
                    context.close();
                }
            } finally {
                mongoTemplate.getDb().drop();
            }
        }
    }

    private boolean isReplicaSetOrMongos(MongoClient client) {
        Document hello = client.getDatabase("admin").runCommand(new Document("hello", 1));
        return hello.get("setName") != null || "isdbgrid".equals(hello.get("msg"));
    }

    private void assertTrend(MeasurementServiceV2Impl service, String taskId, long start, long end,
                             String node, double expected) {
        var trend = service.aggregateTaskMetricsByTaskIdsV2(List.of(taskId), start, end, node);
        assertEquals(start / 1000L, trend.getTs().get(0),
                "first bucket must align with the requested window start for node " + node);
        assertEquals(expected, trend.getOutputQps().get(0), 0.000001D,
                "expected " + expected + " in the first 5s bucket for node " + node + " but got " + trend.getOutputQps());
    }

    private Map<String, String> tags(String taskId, String physicalNodeId, String engineId) {
        return Map.of(
                MetricCons.Tags.F_TYPE, MetricCons.SampleType.TASK.code(),
                MetricCons.Tags.F_TASK_ID, taskId,
                MetricCons.Tags.F_TASK_RECORD_ID, "record-" + physicalNodeId,
                "engineId", engineId,
                "physicalNodeId", physicalNodeId);
    }

    private SampleRequest sample(Map<String, String> tags, long timestamp, double outputQps) {
        Sample sample = new Sample();
        sample.setDate(new Date(timestamp));
        sample.setVs(Map.of(MetricCons.SS.VS.F_OUTPUT_QPS, outputQps,
                MetricCons.SS.VS.F_OUTPUT_SIZE_QPS, outputQps * 2D));
        SampleRequest request = new SampleRequest();
        request.setTags(tags);
        request.setSample(sample);
        return request;
    }

    private ApiMetricsRaw raw(long bucketStart, MetricTypes metricType, String physicalNodeId, long reqCount) {
        ApiMetricsRaw raw = new ApiMetricsRaw();
        raw.setMetricType(metricType.getType());
        raw.setTimeGranularity(TimeGranularity.MINUTE.getType());
        raw.setTimeStart(bucketStart / 1000L);
        raw.setReqCount(reqCount);
        raw.setPhysicalNodeId(physicalNodeId);
        raw.setProcessId(physicalNodeId == null ? null : "process-" + physicalNodeId);
        return raw;
    }

    private QueryBase apiQuery(long startAt, long endAt, String physicalNodeId) {
        QueryBase query = new QueryBase();
        query.setType("range");
        query.setStartAt(startAt);
        query.setEndAt(endAt);
        query.setGranularity(TimeGranularity.MINUTE);
        query.setPhysicalNodeId(physicalNodeId);
        return query;
    }

    private ClusterStateDto clusterState() {
        SystemInfo systemInfo = new SystemInfo("node-a-host", NODE_A, null, null, 0L,
                null, null, PROCESS_A, null, null, null, null, null, null);
        ClusterStateDto state = new ClusterStateDto();
        state.setUuid(NODE_A);
        state.setSystemInfo(systemInfo);
        state.setAgentName("node-a-host");
        Component engine = new Component();
        engine.setStatus("running");
        state.setEngine(engine);
        Component apiServer = new Component();
        apiServer.setStatus("not_deploy");
        state.setApiServer(apiServer);
        return state;
    }

    private AccessNodeInfo accessNode() {
        AccessNodeInfo info = new AccessNodeInfo();
        info.setProcessId(PROCESS_A);
        info.setStatus("running");
        return info;
    }
}
