package com.tapdata.tm.task.service.dashboard;

import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.cluster.dto.AccessNodeInfo;
import com.tapdata.tm.cluster.dto.ClusterStateDto;
import com.tapdata.tm.cluster.dto.Component;
import com.tapdata.tm.cluster.dto.SystemInfo;
import com.tapdata.tm.cluster.service.ClusterStateService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.monitor.vo.TaskMetricsTrendVo;
import com.tapdata.tm.task.service.chart.ChartViewService;
import com.tapdata.tm.task.vo.TaskDashboardVo;
import com.tapdata.tm.v2.api.monitor.main.dto.ApiRequestTrend;
import com.tapdata.tm.v2.api.monitor.main.dto.ServerTopOnHomepage;
import com.tapdata.tm.v2.api.monitor.main.param.QueryBase;
import com.tapdata.tm.v2.api.monitor.service.ApiMetricsChartQuery;
import io.tapdata.common.sample.request.Sample;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskDashboardServiceTest {
    private TaskDashboardService taskDashboardService;
    private ChartViewService chartViewService;
    private MeasurementServiceV2 measurementServiceV2;
    private DataSourceService dataSourceService;
    private ApiMetricsChartQuery apiMetricsChartQuery;
    private UserDetail user;

    @BeforeEach
    void init() {
        taskDashboardService = mock(TaskDashboardService.class);
        chartViewService = mock(ChartViewService.class);
        measurementServiceV2 = mock(MeasurementServiceV2.class);
        dataSourceService = mock(DataSourceService.class);
        apiMetricsChartQuery = mock(ApiMetricsChartQuery.class);
        user = mock(UserDetail.class);

        ReflectionTestUtils.setField(taskDashboardService, "chartViewService", chartViewService);
        ReflectionTestUtils.setField(taskDashboardService, "measurementServiceV2", measurementServiceV2);
        ReflectionTestUtils.setField(taskDashboardService, "dataSourceService", dataSourceService);
        ReflectionTestUtils.setField(taskDashboardService, "apiMetricsChartQuery", apiMetricsChartQuery);
        ReflectionTestUtils.setField(taskDashboardService, "clock", Clock.fixed(Instant.ofEpochSecond(720_000_000L), ZoneOffset.UTC));

        when(taskDashboardService.dashboard(any(UserDetail.class), any(), any(), any(), any())).thenCallRealMethod();
        when(taskDashboardService.dashboard(any(UserDetail.class), any(), any(), any(), any(), any(), any(), any(), any())).thenCallRealMethod();
        when(apiMetricsChartQuery.serverTopOnHomepage(any(QueryBase.class))).thenReturn(ServerTopOnHomepage.create());
        when(apiMetricsChartQuery.homepageRequestTrend(any(QueryBase.class))).thenReturn(ApiRequestTrend.create(TimeGranularity.SECOND_FIVE));
        when(dataSourceService.findAll(any(Query.class))).thenReturn(new ArrayList<>());
        when(dataSourceService.findAllDto(any(Query.class), eq(user))).thenReturn(new ArrayList<>());
    }

    @Test
    void v2DashboardKeepsNullableTrendBucketsAndUsesStoppedTasks() {
        long endAt = 720_000_000L;
        long startAt = endAt - 2L * 60L * 60L;
        TaskDto stoppedTask = task("Stopped task", "stopped", 0L);
        when(chartViewService.getViewTaskDtoByUser(user)).thenReturn(List.of(stoppedTask));

        TaskMetricsTrendVo throughput = new TaskMetricsTrendVo();
        throughput.setTs(List.of(startAt, startAt + 3600L));
        throughput.setOutputQps(Arrays.asList(50D, null));
        throughput.setOutputSizeQps(Arrays.asList(5D, null));
        throughput.setStatus("ok");
        throughput.setTraceability("partial");
        throughput.setCoverage("partial");
        throughput.setHasUnattributedData(true);
        when(measurementServiceV2.aggregateTaskMetricsByTaskIdsV2(anyList(), anyLong(), anyLong(), any()))
                .thenReturn(throughput);

        ApiRequestTrend api = new ApiRequestTrend();
        api.setTs(List.of(startAt, startAt + 3600L));
        api.setValues(Arrays.asList(2D, null));
        api.setStatus("ok");
        api.setTraceability("partial");
        api.setCoverage("partial");
        api.setHasUnattributedData(true);
        when(apiMetricsChartQuery.homepageRequestTrendV2(any(QueryBase.class), any())).thenReturn(api);

        TaskDashboardVo result = taskDashboardService.dashboard(user, "range", null, "trends", null,
                null, startAt, endAt, 2);

        assertEquals(2, result.getQuery().getTrendSchemaVersion());
        assertEquals(3600L, result.getQuery().getIntervalSeconds());
        assertEquals("range", result.getQuery().getType());
        assertEquals(Arrays.asList(50D, null), result.getTrends().getThroughput().getValues());
        assertEquals(Arrays.asList(2D, null), result.getTrends().getApiRequests().getValues());
        assertEquals("partial", result.getTrends().getThroughput().getTraceability());
        assertEquals("partial", result.getTrends().getApiRequests().getTraceability());

        verify(measurementServiceV2).aggregateTaskMetricsByTaskIdsV2(
                eq(List.of(stoppedTask.getId().toHexString())), eq(startAt * 1000L), eq(endAt * 1000L), eq(null));
        ArgumentCaptor<QueryBase> apiQuery = ArgumentCaptor.forClass(QueryBase.class);
        verify(apiMetricsChartQuery).homepageRequestTrendV2(apiQuery.capture(), eq(null));
        assertEquals("range", apiQuery.getValue().getType());
        assertEquals(startAt, apiQuery.getValue().getStartAt());
        assertEquals(endAt, apiQuery.getValue().getEndAt());
        assertEquals(TimeGranularity.HOUR, apiQuery.getValue().getGranularity());
    }

    @Test
    void v2DashboardRejectsNonTrendRequests() {
        assertThrows(BizException.class, () -> taskDashboardService.dashboard(user, "days", 1L, "summary", null,
                null, 100L, 200L, 2));
    }

    @Test
    void rangeTypeRequiresExplicitV2SchemaAndTimes() {
        assertThrows(BizException.class, () -> taskDashboardService.dashboard(user, "range", null, "trends", null,
                null, null, null, null));
        assertThrows(BizException.class, () -> taskDashboardService.dashboard(user, "range", null, "trends", null,
                null, null, null, 2));
    }

    @Test
    void dashboardNodesUsesComponentStatusForVisibleInstallations() {
        ClusterStateService clusterStateService = mock(ClusterStateService.class);
        ReflectionTestUtils.setField(taskDashboardService, "clusterStateService", clusterStateService);
        when(taskDashboardService.dashboardNodes(any(UserDetail.class))).thenCallRealMethod();
        AccessNodeInfo offlineWorker = new AccessNodeInfo("process-1", "host-1", "ip-1", "stopped");
        AccessNodeInfo secondVisibleWorker = new AccessNodeInfo("process-2", "host-2", "ip-2", "running");
        when(clusterStateService.findAccessNodeInfo(user)).thenReturn(List.of(offlineWorker, secondVisibleWorker));

        SystemInfo engineSystemInfo = new SystemInfo("host-1", "node-uuid-123456", null, new ArrayList<>(),
                0L, null, null, "process-1", null, null, null, null, null, null);
        ClusterStateDto engineOnlyState = new ClusterStateDto();
        engineOnlyState.setUuid("node-uuid-123456");
        engineOnlyState.setAgentName("agent-1");
        engineOnlyState.setSystemInfo(engineSystemInfo);
        Component deployedEngine = new Component();
        deployedEngine.setStatus("running");
        Component undeployedApiServer = new Component();
        undeployedApiServer.setStatus("not_deploy");
        engineOnlyState.setEngine(deployedEngine);
        engineOnlyState.setApiServer(undeployedApiServer);

        SystemInfo apiSystemInfo = new SystemInfo("host-2", "node-uuid-654321", null, new ArrayList<>(),
                0L, null, null, "process-2", null, null, null, null, null, null);
        ClusterStateDto secondNodeState = new ClusterStateDto();
        secondNodeState.setUuid("node-uuid-654321");
        secondNodeState.setAgentName("agent-2");
        secondNodeState.setSystemInfo(apiSystemInfo);
        Component undeployedEngine = new Component();
        undeployedEngine.setStatus("not_deploy");
        Component deployedApiServer = new Component();
        deployedApiServer.setStatus("running");
        secondNodeState.setEngine(undeployedEngine);
        secondNodeState.setApiServer(deployedApiServer);
        when(clusterStateService.findAll(any(Query.class))).thenReturn(List.of(engineOnlyState, secondNodeState));

        List<TaskDashboardVo.NodeOption> nodes = taskDashboardService.dashboardNodes(user);

        assertEquals(2, nodes.size());
        assertEquals("node-uuid-123456", nodes.get(0).getPhysicalNodeId());
        assertEquals("offline", nodes.get(0).getStatus());
        assertEquals("agent-1 (123456)", nodes.get(0).getLabel());
        assertEquals(true, nodes.get(0).isEngineDeployed());
        assertEquals(false, nodes.get(0).isApiServerDeployed());
        assertEquals("node-uuid-654321", nodes.get(1).getPhysicalNodeId());
        assertEquals(true, nodes.get(1).isApiServerDeployed());
        assertEquals(false, nodes.get(1).isEngineDeployed());
    }

    @Test
    void selectedNodeQueryUsesOnlyTargetProcessesAndMergesAuthorizedComponents() {
        ClusterStateService clusterStateService = mock(ClusterStateService.class);
        ReflectionTestUtils.setField(taskDashboardService, "clusterStateService", clusterStateService);
        ClusterStateDto first = dashboardState("selected-node", "selected-node", "process-1", true, false);
        ClusterStateDto second = dashboardState(null, "selected-node", "process-2", false, true);
        when(clusterStateService.findAll(any(Query.class))).thenReturn(List.of(first, second));
        when(clusterStateService.findAccessNodeInfo(eq(user), anyCollection())).thenReturn(List.of(
                new AccessNodeInfo("process-1", "host", "ip", "stopped"),
                new AccessNodeInfo("process-2", "host", "ip", "running")));

        TaskDashboardVo.NodeOption selected = ReflectionTestUtils.invokeMethod(
                taskDashboardService, "findAuthorizedNode", user, "selected-node");

        assertEquals("selected-node", selected.getPhysicalNodeId());
        assertEquals("running", selected.getStatus());
        assertTrue(selected.isEngineDeployed());
        assertTrue(selected.isApiServerDeployed());
        ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
        verify(clusterStateService).findAll(query.capture());
        String criteria = query.getValue().getQueryObject().toJson();
        assertTrue(criteria.contains("uuid"));
        assertTrue(criteria.contains("systemInfo.uuid"));
        assertTrue(criteria.contains("selected-node"));
        @SuppressWarnings({"unchecked", "rawtypes"})
        ArgumentCaptor<Collection<String>> processIds = (ArgumentCaptor) ArgumentCaptor.forClass(Collection.class);
        verify(clusterStateService).findAccessNodeInfo(eq(user), processIds.capture());
        assertEquals(Set.of("process-1", "process-2"), new HashSet<>(processIds.getValue()));
        verify(clusterStateService, never()).findAccessNodeInfo(user);
    }

    @Test
    void selectedNodeIgnoresProcessesOutsideTheUserAccessScope() {
        ClusterStateService clusterStateService = mock(ClusterStateService.class);
        ReflectionTestUtils.setField(taskDashboardService, "clusterStateService", clusterStateService);
        when(clusterStateService.findAll(any(Query.class))).thenReturn(List.of(
                dashboardState("selected-node", "selected-node", "allowed", false, true),
                dashboardState("selected-node", "selected-node", "denied", true, false)));
        when(clusterStateService.findAccessNodeInfo(eq(user), anyCollection())).thenReturn(List.of(
                new AccessNodeInfo("allowed", "host", "ip", "running")));

        TaskDashboardVo.NodeOption selected = ReflectionTestUtils.invokeMethod(
                taskDashboardService, "findAuthorizedNode", user, "selected-node");

        assertEquals(false, selected.isEngineDeployed());
        assertTrue(selected.isApiServerDeployed());
    }

    @Test
    void selectedNodeDoesNotTreatEmptyGroupAsAnAuthorizedProcess() {
        ClusterStateService clusterStateService = mock(ClusterStateService.class);
        ReflectionTestUtils.setField(taskDashboardService, "clusterStateService", clusterStateService);
        when(clusterStateService.findAll(any(Query.class))).thenReturn(List.of(
                dashboardState("selected-node", "selected-node", "allowed", false, true),
                dashboardState("selected-node", "selected-node", "denied", true, false)));
        AccessNodeInfo emptyGroup = new AccessNodeInfo();
        emptyGroup.setProcessId("denied");
        emptyGroup.setAccessNodeType("MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP");
        emptyGroup.setAccessNodes(List.of());
        when(clusterStateService.findAccessNodeInfo(eq(user), anyCollection())).thenReturn(List.of(
                emptyGroup, new AccessNodeInfo("allowed", "host", "ip", "running")));

        TaskDashboardVo.NodeOption selected = ReflectionTestUtils.invokeMethod(
                taskDashboardService, "findAuthorizedNode", user, "selected-node");

        assertEquals(false, selected.isEngineDeployed());
    }

    @Test
    void selectedNodeRejectsMissingAndUnauthorizedNodes() {
        ClusterStateService clusterStateService = mock(ClusterStateService.class);
        ReflectionTestUtils.setField(taskDashboardService, "clusterStateService", clusterStateService);
        when(clusterStateService.findAll(any(Query.class))).thenReturn(List.of(), List.of(
                dashboardState("selected-node", "selected-node", "denied", true, false)));
        when(clusterStateService.findAccessNodeInfo(eq(user), anyCollection())).thenReturn(List.of());

        BizException missing = assertThrows(BizException.class, () -> ReflectionTestUtils.invokeMethod(
                taskDashboardService, "findAuthorizedNode", user, "selected-node"));
        assertEquals("server.not.exists", missing.getArgs()[0]);
        verify(clusterStateService, never()).findAccessNodeInfo(eq(user), anyCollection());
        BizException denied = assertThrows(BizException.class, () -> ReflectionTestUtils.invokeMethod(
                taskDashboardService, "findAuthorizedNode", user, "selected-node"));
        assertEquals("server.not.exists", denied.getArgs()[0]);
    }

    @Test
    void selectedNodeUsesSystemUuidWhenPrimaryUuidIsBlank() {
        ClusterStateService clusterStateService = mock(ClusterStateService.class);
        ReflectionTestUtils.setField(taskDashboardService, "clusterStateService", clusterStateService);
        when(clusterStateService.findAll(any(Query.class))).thenReturn(List.of(
                dashboardState("", "selected-node", "process-1", true, false)));
        when(clusterStateService.findAccessNodeInfo(eq(user), anyCollection())).thenReturn(List.of(
                new AccessNodeInfo("process-1", "host", "ip", "running")));

        TaskDashboardVo.NodeOption selected = ReflectionTestUtils.invokeMethod(
                taskDashboardService, "findAuthorizedNode", user, "selected-node");

        assertEquals("selected-node", selected.getPhysicalNodeId());
        assertTrue(selected.isEngineDeployed());
    }

    @Test
    void selectedNodeRejectsSystemUuidWhenPrimaryUuidDiffers() {
        ClusterStateService clusterStateService = mock(ClusterStateService.class);
        ReflectionTestUtils.setField(taskDashboardService, "clusterStateService", clusterStateService);
        when(clusterStateService.findAll(any(Query.class))).thenReturn(List.of(
                dashboardState("other-node", "selected-node", "process-1", true, false)));
        when(clusterStateService.findAccessNodeInfo(eq(user), anyCollection())).thenReturn(List.of(
                new AccessNodeInfo("process-1", "host", "ip", "running")));

        BizException error = assertThrows(BizException.class, () -> ReflectionTestUtils.invokeMethod(
                taskDashboardService, "findAuthorizedNode", user, "selected-node"));
        assertEquals("server.not.exists", error.getArgs()[0]);
    }

    private ClusterStateDto dashboardState(String uuid, String systemUuid, String processId,
                                           boolean engineDeployed, boolean apiDeployed) {
        ClusterStateDto state = new ClusterStateDto();
        state.setUuid(uuid);
        state.setAgentName("agent");
        state.setSystemInfo(new SystemInfo("host", systemUuid, null, new ArrayList<>(),
                0L, null, null, processId, null, null, null, null, null, null));
        Component engine = new Component();
        engine.setStatus(engineDeployed ? "running" : "not_deploy");
        Component api = new Component();
        api.setStatus(apiDeployed ? "running" : "not_deploy");
        state.setEngine(engine);
        state.setApiServer(api);
        return state;
    }

    @Test
    void testDashboardFallbackToDefaultWindowWhenInvalidAndEmptyTasks() {
        when(chartViewService.getViewTaskDtoByUser(user)).thenReturn(new ArrayList<>());
        when(measurementServiceV2.findLastMinuteSamplesByTaskIds(any())).thenReturn(new HashMap<>());
        when(measurementServiceV2.aggregateTaskMetricsByTaskIds(any(), anyLong(), anyLong())).thenReturn(new TaskMetricsTrendVo());

        TaskDashboardVo result = taskDashboardService.dashboard(user, "invalid", 99L, null, null);

        assertNotNull(result);
        assertEquals("minute", result.getQuery().getType());
        assertEquals(5L, result.getQuery().getStep());
        assertEquals(0, result.getSummary().getActiveTasks().getTotal());
        assertEquals(0D, result.getSummary().getTotalThroughput().getCurrent());
        assertEquals(0, result.getSummary().getConnectedDbs().getTotal());
        assertEquals(0, result.getTops().getTopLaggingTasks().size());
        assertEquals(0, result.getTops().getTopThroughputTasks().size());
    }

    @Test
    void testDashboardFillsThroughputTrendTimelineWhenMetricsEmpty() {
        when(chartViewService.getViewTaskDtoByUser(user)).thenReturn(new ArrayList<>());
        when(measurementServiceV2.findLastMinuteSamplesByTaskIds(any())).thenReturn(new HashMap<>());
        when(measurementServiceV2.aggregateTaskMetricsByTaskIds(any(), anyLong(), anyLong())).thenReturn(new TaskMetricsTrendVo());

        TaskDashboardVo result = taskDashboardService.dashboard(user, "minute", 5L, null, null);

        assertFilledZeroTrend(result.getTrends().getThroughput(), result.getQuery().getStartAt(), result.getQuery().getEndAt(), 5L);
    }

    @Test
    void testDashboardFillsApiRequestTrendTimelineWhenMetricsEmpty() {
        when(chartViewService.getViewTaskDtoByUser(user)).thenReturn(new ArrayList<>());
        when(measurementServiceV2.findLastMinuteSamplesByTaskIds(any())).thenReturn(new HashMap<>());
        when(measurementServiceV2.aggregateTaskMetricsByTaskIds(any(), anyLong(), anyLong())).thenReturn(new TaskMetricsTrendVo());
        when(apiMetricsChartQuery.homepageRequestTrend(any(QueryBase.class))).thenReturn(ApiRequestTrend.create(TimeGranularity.SECOND_FIVE));

        TaskDashboardVo result = taskDashboardService.dashboard(user, "minute", 5L, null, null);

        assertFilledZeroTrend(result.getTrends().getApiRequests(), result.getQuery().getStartAt(), result.getQuery().getEndAt(), 5L);
    }

    @Test
    void testDashboardUsesMeasurementsForLagAndThroughput() {
        TaskDto lagTask = task("Lag Task", TaskDto.STATUS_RUNNING, 999_999L);
        TaskDto fastTask = task("Fast Task", TaskDto.STATUS_RUNNING, 888_888L);
        TaskDto errorTask = task("Error Task", TaskDto.STATUS_ERROR, 777_777L);
        TaskDto noSampleTask = task("No Sample Task", TaskDto.STATUS_RUNNING, 666_666L);
        when(chartViewService.getViewTaskDtoByUser(user)).thenReturn(List.of(lagTask, fastTask, errorTask, noSampleTask));

        Map<String, Sample> samples = new HashMap<>();
        samples.put(lagTask.getId().toHexString(), sample(2_000L, 50D));
        samples.put(fastTask.getId().toHexString(), sample(1_000L, 70D));
        when(measurementServiceV2.findLastMinuteSamplesByTaskIds(any())).thenReturn(samples);

        TaskMetricsTrendVo trendVo = new TaskMetricsTrendVo();
        trendVo.setTs(List.of(1_000L, 2_000L));
        trendVo.setOutputQps(List.of(100D, 150D));
        trendVo.setOutputSizeQps(List.of(10D, 15D));
        when(measurementServiceV2.aggregateTaskMetricsByTaskIds(any(), anyLong(), anyLong())).thenReturn(trendVo);

        TaskDashboardVo result = taskDashboardService.dashboard(user, "hours", 1L, null, null);

        assertEquals("hours", result.getQuery().getType());
        assertEquals(1L, result.getQuery().getStep());
        assertEquals(4, result.getSummary().getActiveTasks().getTotal());
        assertEquals(3, result.getSummary().getActiveTasks().getRunning());
        assertEquals(1, result.getSummary().getActiveTasks().getError());
        assertEquals(2_000L, result.getSummary().getActiveTasks().getMaxLag());
        assertEquals(1_000L, result.getSummary().getActiveTasks().getMinLag());
        assertEquals(150D, result.getSummary().getTotalThroughput().getCurrent());
        assertEquals(150D, result.getSummary().getTotalThroughput().getPeak());
        assertEquals(15D, result.getSummary().getTotalThroughput().getDataRate());
        assertEquals(50D, result.getSummary().getTotalThroughput().getChangeRate());
        assertEquals(List.of(1_000L, 2_000L), result.getTrends().getThroughput().getTs());
        assertEquals(List.of(100D, 150D), result.getTrends().getThroughput().getValues());
        assertEquals(2, result.getTops().getTopLaggingTasks().size());
        assertEquals(2, result.getTops().getTopThroughputTasks().size());
        assertEquals("Lag Task", result.getTops().getTopLaggingTasks().get(0).getTaskName());
        assertEquals("Fast Task", result.getTops().getTopThroughputTasks().get(0).getTaskName());
    }

    @Test
    void testDashboardUsesVisibleConnectionsSortedByTableCount() {
        when(chartViewService.getViewTaskDtoByUser(user)).thenReturn(new ArrayList<>());
        when(measurementServiceV2.findLastMinuteSamplesByTaskIds(any())).thenReturn(new HashMap<>());
        when(measurementServiceV2.aggregateTaskMetricsByTaskIds(any(), anyLong(), anyLong())).thenReturn(new TaskMetricsTrendVo());

        DataSourceConnectionDto slow = connection("slow", 10L);
        DataSourceConnectionDto fast = connection("fast", 20L);
        DataSourceConnectionDto sameCount = connection("alpha", 20L);
        DataSourceConnectionDto third = connection("beta", 15L);
        when(dataSourceService.findAllDto(any(Query.class), eq(user))).thenReturn(List.of(slow, fast, sameCount, third));

        TaskDashboardVo result = taskDashboardService.dashboard(user, null, null, null, null);

        assertEquals(4, result.getSummary().getConnectedDbs().getTotal());
        assertEquals(3, result.getSummary().getConnectedDbs().getItems().size());
        assertEquals("alpha", result.getSummary().getConnectedDbs().getItems().get(0).getName());
        assertEquals("fast", result.getSummary().getConnectedDbs().getItems().get(1).getName());
        assertEquals("beta", result.getSummary().getConnectedDbs().getItems().get(2).getName());
    }

    @Test
    void testDashboardMapsApiSummaryAndTrend() {
        when(chartViewService.getViewTaskDtoByUser(user)).thenReturn(new ArrayList<>());
        when(measurementServiceV2.findLastMinuteSamplesByTaskIds(any())).thenReturn(new HashMap<>());
        when(measurementServiceV2.aggregateTaskMetricsByTaskIds(any(), anyLong(), anyLong())).thenReturn(new TaskMetricsTrendVo());

        ServerTopOnHomepage homepage = ServerTopOnHomepage.create();
        homepage.setTotalRequestCount(1_200L);
        homepage.setErrorCount(12L);
        homepage.setTotalErrorRate(1D);
        homepage.setResponseTimeAvg(124D);
        when(apiMetricsChartQuery.serverTopOnHomepage(any(QueryBase.class))).thenReturn(homepage);

        ApiRequestTrend trend = ApiRequestTrend.create(TimeGranularity.SECOND_FIVE);
        trend.setTs(List.of(10L, 20L));
        trend.setValues(List.of(40D, 50D));
        when(apiMetricsChartQuery.homepageRequestTrend(any(QueryBase.class))).thenReturn(trend);

        TaskDashboardVo result = taskDashboardService.dashboard(user, "days", 1L, null, null);

        assertEquals("days", result.getQuery().getType());
        assertEquals(1L, result.getQuery().getStep());
        assertEquals(1_200L, result.getSummary().getApiRequests().getTotal());
        assertEquals(12L, result.getSummary().getApiRequests().getFailed());
        assertEquals(1D, result.getSummary().getApiRequests().getErrorRate());
        assertEquals(124D, result.getSummary().getApiRequests().getAvgTime());
        assertEquals(List.of(10L, 20L), result.getTrends().getApiRequests().getTs());
        assertEquals(List.of(40D, 50D), result.getTrends().getApiRequests().getValues());
    }

    @Test
    void testDashboardUsesLatestSamplesByWindowForLagAndTops() {
        TaskDto task = task("Window Task", TaskDto.STATUS_RUNNING, 999_999L);
        when(chartViewService.getViewTaskDtoByUser(user)).thenReturn(List.of(task));

        Map<String, Sample> samples = new HashMap<>();
        samples.put(task.getId().toHexString(), sample(3_000L, 80D));
        when(measurementServiceV2.findLastMinuteSamplesByTaskIds(any())).thenReturn(samples);
        when(measurementServiceV2.aggregateTaskMetricsByTaskIds(any(), anyLong(), anyLong())).thenReturn(new TaskMetricsTrendVo());

        TaskDashboardVo result = taskDashboardService.dashboard(user, "days", 1L, "tops", 10);

        assertEquals(1, result.getTops().getTopLaggingTasks().size());
        assertEquals("Window Task", result.getTops().getTopLaggingTasks().get(0).getTaskName());
        assertEquals(3_000L, result.getTops().getTopLaggingTasks().get(0).getLatency());
    }

    @Test
    void testDashboardSupportsApiRequestsOnly() {
        when(chartViewService.getViewTaskDtoByUser(user)).thenReturn(new ArrayList<>());
        when(measurementServiceV2.findLastMinuteSamplesByTaskIds(any())).thenReturn(new HashMap<>());
        when(measurementServiceV2.aggregateTaskMetricsByTaskIds(any(), anyLong(), anyLong())).thenReturn(new TaskMetricsTrendVo());

        ServerTopOnHomepage homepage = ServerTopOnHomepage.create();
        homepage.setTotalRequestCount(300L);
        homepage.setErrorCount(3L);
        when(apiMetricsChartQuery.serverTopOnHomepage(any(QueryBase.class))).thenReturn(homepage);

        ApiRequestTrend trend = ApiRequestTrend.create(TimeGranularity.SECOND_FIVE);
        trend.setTs(List.of(1L, 2L));
        trend.setValues(List.of(9D, 8D));
        when(apiMetricsChartQuery.homepageRequestTrend(any(QueryBase.class))).thenReturn(trend);

        TaskDashboardVo result = taskDashboardService.dashboard(user, "minute", 5L, "apiRequests", null);

        assertEquals(300L, result.getSummary().getApiRequests().getTotal());
        assertEquals(3L, result.getSummary().getApiRequests().getFailed());
        assertEquals(List.of(1L, 2L), result.getTrends().getApiRequests().getTs());
        assertEquals(0D, result.getSummary().getTotalThroughput().getCurrent());
        assertEquals(0, result.getTops().getTopLaggingTasks().size());
    }

    @Test
    void testDashboardSupportsTopsLimit() {
        List<TaskDto> tasks = new ArrayList<>();
        Map<String, Sample> samples = new HashMap<>();
        for (int i = 0; i < 6; i++) {
            TaskDto task = task("Task " + i, TaskDto.STATUS_RUNNING, i);
            tasks.add(task);
            samples.put(task.getId().toHexString(), sample(6L - i, 100D - i));
        }
        when(chartViewService.getViewTaskDtoByUser(user)).thenReturn(tasks);
        when(measurementServiceV2.findLastMinuteSamplesByTaskIds(any())).thenReturn(samples);
        when(measurementServiceV2.aggregateTaskMetricsByTaskIds(any(), anyLong(), anyLong())).thenReturn(new TaskMetricsTrendVo());

        TaskDashboardVo result = taskDashboardService.dashboard(user, null, null, "tops", 5);

        assertEquals(5, result.getTops().getTopLaggingTasks().size());
        assertEquals(5, result.getTops().getTopThroughputTasks().size());
        assertEquals(0L, result.getSummary().getApiRequests().getTotal());
        assertEquals(0, result.getSummary().getConnectedDbs().getItems().size());
    }

    private TaskDto task(String name, String status, long delayTime) {
        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        taskDto.setName(name);
        taskDto.setStatus(status);
        taskDto.setDelayTime(delayTime);
        taskDto.setTaskRecordId(new ObjectId().toHexString());
        return taskDto;
    }

    private Sample sample(long lag, double outputQps) {
        Sample sample = new Sample();
        Map<String, Number> values = new HashMap<>();
        values.put("replicateLag", lag);
        values.put("outputQps", outputQps);
        sample.setVs(values);
        return sample;
    }

    private DataSourceConnectionDto connection(String name, Long tableCount) {
        DataSourceConnectionDto dto = new DataSourceConnectionDto();
        dto.setId(new ObjectId());
        dto.setName(name);
        dto.setTableCount(tableCount);
        return dto;
    }

    private void assertFilledZeroTrend(TaskDashboardVo.Trend trend, long startAt, long endAt, long stepSeconds) {
        assertNotNull(trend.getTs());
        assertNotNull(trend.getValues());
        assertEquals(trend.getTs().size(), trend.getValues().size());
        assertEquals((endAt / stepSeconds) - (startAt / stepSeconds) + 1, trend.getTs().size());
        assertEquals((startAt / stepSeconds) * stepSeconds, trend.getTs().get(0));
        assertEquals((endAt / stepSeconds) * stepSeconds, trend.getTs().get(trend.getTs().size() - 1));
        for (int i = 1; i < trend.getTs().size(); i++) {
            assertEquals(stepSeconds, trend.getTs().get(i) - trend.getTs().get(i - 1));
        }
        trend.getValues().forEach(value -> assertEquals(0D, value));
    }
}
