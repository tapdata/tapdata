package com.tapdata.tm.task.service.dashboard;

import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
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
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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

        when(taskDashboardService.dashboard(any(UserDetail.class), any(), any(), any(), any())).thenCallRealMethod();
        when(apiMetricsChartQuery.serverTopOnHomepage(any(QueryBase.class))).thenReturn(ServerTopOnHomepage.create());
        when(apiMetricsChartQuery.homepageRequestTrend(any(QueryBase.class))).thenReturn(ApiRequestTrend.create(TimeGranularity.SECOND_FIVE));
        when(dataSourceService.findAllDto(any(Query.class), eq(user))).thenReturn(new ArrayList<>());
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
}
