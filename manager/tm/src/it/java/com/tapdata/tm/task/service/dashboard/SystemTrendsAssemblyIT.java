package com.tapdata.tm.task.service.dashboard;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.cluster.dto.AccessNodeInfo;
import com.tapdata.tm.cluster.dto.ClusterStateDto;
import com.tapdata.tm.cluster.dto.Component;
import com.tapdata.tm.cluster.dto.SystemInfo;
import com.tapdata.tm.cluster.service.ClusterStateService;
import com.tapdata.tm.commons.metrics.MetricCons;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.service.MeasurementServiceV2Impl;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.chart.ChartViewService;
import com.tapdata.tm.task.vo.TaskDashboardVo;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import com.tapdata.tm.v2.api.monitor.main.param.QueryBase;
import com.tapdata.tm.v2.api.monitor.service.ApiMetricsChartQuery;
import com.tapdata.tm.v2.api.monitor.service.ApiMetricsRawMergeService;
import com.tapdata.tm.v2.api.monitor.service.ApiMetricsRawService;
import io.tapdata.common.sample.request.Sample;
import io.tapdata.common.sample.request.SampleRequest;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 系统趋势真实装配验证（L2）。
 *
 * <p>真实 Mongo 下验证 measurement 与 API 查询链路、节点归属、无身份数据标记，
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
                "TAPDATA_TEST_MONGO_URI must point to an isolated Mongo database for this integration test");
        Assumptions.assumeTrue("true".equalsIgnoreCase(System.getenv("TAPDATA_TEST_MONGO_ALLOW_TEMP_DATABASE")),
                "TAPDATA_TEST_MONGO_ALLOW_TEMP_DATABASE=true is required because the test creates and drops a random database");

        MongoClient client = MongoClients.create(mongoUri);
        String databaseName = "tap12023_trends_" + UUID.randomUUID().toString().replace("-", "");
        MongoTemplate mongoTemplate = new MongoTemplate(new SimpleMongoClientDatabaseFactory(client, databaseName));
        try {
            MeasurementServiceV2Impl measurementService = new MeasurementServiceV2Impl(
                    CompletableFuture.completedFuture(mongoTemplate),
                    mock(MetadataInstancesService.class), mock(TaskService.class));

            String taskId = new ObjectId().toHexString();
            long nowMillis = System.currentTimeMillis();
            long minuteStart = nowMillis - nowMillis % 60_000L - 60_000L;
            long sampleStart = minuteStart + 1_000L;

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

            ApiMetricsRawService rawService = new ApiMetricsRawService();
            ReflectionTestUtils.setField(rawService, "mongoTemplate", mongoTemplate);
            ApiMetricsChartQuery chartQuery = new ApiMetricsChartQuery();
            ReflectionTestUtils.setField(chartQuery, "service", rawService);
            ApiMetricsRawMergeService mergeService = mock(ApiMetricsRawMergeService.class);
            when(mergeService.getDelay()).thenReturn(0L);
            ReflectionTestUtils.setField(chartQuery, "metricsRawMergeService", mergeService);

            long apiBucket = minuteStart - 3_600_000L;
            long untraceableBucket = apiBucket + 60_000L;
            mongoTemplate.save(raw(apiBucket, MetricTypes.SERER, NODE_A, 10L));
            mongoTemplate.save(raw(apiBucket, MetricTypes.SERER, NODE_B, 20L));
            mongoTemplate.save(raw(apiBucket, MetricTypes.SERER, null, 5L));
            mongoTemplate.save(raw(apiBucket, MetricTypes.ALL, null, 35L));
            mongoTemplate.save(raw(untraceableBucket, MetricTypes.SERER, null, 7L));

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

            TaskDashboardService dashboardService = new TaskDashboardService();
            ReflectionTestUtils.setField(dashboardService, "clock", Clock.fixed(Instant.ofEpochMilli(minuteStart + 120_000L), ZoneOffset.UTC));
            ReflectionTestUtils.setField(dashboardService, "measurementServiceV2", measurementService);
            ReflectionTestUtils.setField(dashboardService, "apiMetricsChartQuery", chartQuery);

            ChartViewService chartViewService = mock(ChartViewService.class);
            TaskDto task = new TaskDto();
            task.setId(new ObjectId(taskId));
            task.setStatus(TaskDto.STATUS_RUNNING);
            when(chartViewService.getViewTaskDtoByUser(any())).thenReturn(List.of(task));
            ReflectionTestUtils.setField(dashboardService, "chartViewService", chartViewService);

            DataSourceService dataSourceService = mock(DataSourceService.class);
            when(dataSourceService.findAll(any(Query.class))).thenReturn(new ArrayList<>());
            ReflectionTestUtils.setField(dashboardService, "dataSourceService", dataSourceService);

            ClusterStateService clusterStateService = mock(ClusterStateService.class);
            ClusterStateDto state = clusterState();
            when(clusterStateService.findAll(any(Query.class))).thenReturn(List.of(state));
            when(clusterStateService.findAccessNodeInfo(any(), anyCollection())).thenReturn(List.of(accessNode()));
            ReflectionTestUtils.setField(dashboardService, "clusterStateService", clusterStateService);

            UserDetail user = new UserDetail("user-a", "customer", "user-a", "password", List.of());
            TaskDashboardVo nodeDashboard = dashboardService.dashboard(user, "range", 3600L, "trends", 5,
                    NODE_A, minuteStart / 1000L, rangeEnd / 1000L, 2);
            assertNotNull(nodeDashboard.getTrends().getThroughput());
            assertTrue(nodeDashboard.getTrends().getThroughput().getValues().stream()
                    .filter(java.util.Objects::nonNull).anyMatch(value -> Math.abs(value - 5D) < 0.000001D));
            assertEquals("component_not_deployed", nodeDashboard.getTrends().getApiRequests().getStatus());

            TaskDashboardVo allDashboard = dashboardService.dashboard(user, "range", 3600L, "trends", 5,
                    null, minuteStart / 1000L, rangeEnd / 1000L, 2);
            assertTrue(allDashboard.getTrends().getThroughput().getValues().stream()
                    .filter(java.util.Objects::nonNull).anyMatch(value -> Math.abs(value - 15D) < 0.000001D));
        } finally {
            try {
                mongoTemplate.getDb().drop();
            } finally {
                client.close();
            }
        }
    }

    private void assertTrend(MeasurementServiceV2Impl service, String taskId, long start, long end,
                             String node, double expected) {
        var trend = service.aggregateTaskMetricsByTaskIdsV2(List.of(taskId), start, end, node);
        assertTrue(trend.getOutputQps().stream()
                        .filter(java.util.Objects::nonNull)
                        .anyMatch(value -> Math.abs(value - expected) < 0.000001D),
                "expected " + expected + " for node " + node + " but got " + trend.getOutputQps());
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
