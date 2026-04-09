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
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Setter(onMethod_ = {@Autowired})
public class TaskDashboardService {
    private static final String DEFAULT_TYPE = "minute";
    private static final long DEFAULT_STEP = 5L;
    private static final int DEFAULT_TOP_LIMIT = 5;
    private static final int CONNECTED_DB_LIMIT = 3;

    enum DashboardType {
        ALL("all"),
        SUMMARY("summary"),
        ACTIVE_TASKS("activeTasks"),
        TOTAL_THROUGHPUT("totalThroughput"),
        CONNECTED_DBS("connectedDbs"),
        API_REQUESTS("apiRequests"),
        TRENDS("trends"),
        TOPS("tops");

        private static final Set<DashboardType> NEED_ACTIVE_TASKS = EnumSet.of(ALL, SUMMARY, ACTIVE_TASKS);
        private static final Set<DashboardType> NEED_THROUGHPUT_SUMMARY = EnumSet.of(ALL, SUMMARY, TOTAL_THROUGHPUT);
        private static final Set<DashboardType> NEED_THROUGHPUT_TREND = EnumSet.of(ALL, TRENDS);
        private static final Set<DashboardType> NEED_CONNECTED_DBS = EnumSet.of(ALL, SUMMARY, CONNECTED_DBS);
        private static final Set<DashboardType> NEED_API_SUMMARY = EnumSet.of(ALL, SUMMARY, API_REQUESTS);
        private static final Set<DashboardType> NEED_API_TREND = EnumSet.of(ALL, TRENDS, API_REQUESTS);
        private static final Set<DashboardType> NEED_TOPS = EnumSet.of(ALL, TOPS);

        private final String value;

        DashboardType(String value) {
            this.value = value;
        }

        String value() {
            return value;
        }

        boolean needActiveTasks() { return NEED_ACTIVE_TASKS.contains(this); }
        boolean needThroughputSummary() { return NEED_THROUGHPUT_SUMMARY.contains(this); }
        boolean needThroughputTrend() { return NEED_THROUGHPUT_TREND.contains(this); }
        boolean needThroughput() { return needThroughputSummary() || needThroughputTrend(); }
        boolean needConnectedDbs() { return NEED_CONNECTED_DBS.contains(this); }
        boolean needApiSummary() { return NEED_API_SUMMARY.contains(this); }
        boolean needApiTrend() { return NEED_API_TREND.contains(this); }
        boolean needApiRequests() { return needApiSummary() || needApiTrend(); }
        boolean needTops() { return NEED_TOPS.contains(this); }

        static DashboardType fromValue(String input) {
            if (StringUtils.isBlank(input)) {
                return ALL;
            }
            String trimmed = input.trim();
            for (DashboardType dt : values()) {
                if (dt.value.equals(trimmed)) {
                    return dt;
                }
            }
            return ALL;
        }
    }

    private ChartViewService chartViewService;
    private MeasurementServiceV2 measurementServiceV2;
    private DataSourceService dataSourceService;
    private ApiMetricsChartQuery apiMetricsChartQuery;

    public TaskDashboardVo dashboard(UserDetail user, String type, Long step, String dashboardType, Integer top) {
        DashboardQuery dashboardQuery = normalize(type, step, dashboardType, top);
        DashboardType dt = dashboardQuery.dashboardTypeEnum;
        TaskDashboardVo result = initResult(dashboardQuery);

        boolean needTaskList = dt.needActiveTasks() || dt.needTops() || dt.needThroughput();
        boolean needLatestSamples = dt.needActiveTasks() || dt.needTops();
        boolean needTaskTrend = dt.needThroughput();

        List<TaskDto> tasks = needTaskList
                ? Optional.ofNullable(chartViewService.getViewTaskDtoByUser(user)).orElseGet(ArrayList::new)
                : new ArrayList<>();
        List<TaskDto> runningTasks = tasks.stream()
                .filter(Objects::nonNull)
                .filter(task -> TaskDto.STATUS_RUNNING.equals(task.getStatus()))
                .collect(Collectors.toList());
        List<String> runningTaskIds = runningTasks.stream()
                .map(TaskDto::getId)
                .filter(Objects::nonNull)
                .map(id -> id.toHexString())
                .collect(Collectors.toList());

        Map<String, Sample> latestSamples = needLatestSamples && CollectionUtils.isNotEmpty(runningTaskIds)
                ? Optional.ofNullable(measurementServiceV2.findLastMinuteSamplesByTaskIds(runningTaskIds)).orElseGet(HashMap::new)
                : new HashMap<>();
        TaskMetricsTrendVo trendVo = needTaskTrend && CollectionUtils.isNotEmpty(runningTaskIds)
                ? Optional.ofNullable(measurementServiceV2.aggregateTaskMetricsByTaskIds(runningTaskIds, dashboardQuery.startAtMs, dashboardQuery.endAtMs))
                .orElseGet(TaskMetricsTrendVo::new)
                : new TaskMetricsTrendVo();

        if (dt.needActiveTasks()) {
            fillActiveTasks(result.getSummary().getActiveTasks(), tasks, latestSamples);
        }
        if (dt.needThroughput()) {
            fillThroughput(result.getSummary().getTotalThroughput(), result.getTrends().getThroughput(), trendVo, dt.needThroughputSummary(), dt.needThroughputTrend());
        }
        if (dt.needConnectedDbs()) {
            fillConnectedDbs(result.getSummary().getConnectedDbs(), user);
        }
        if (dt.needApiRequests()) {
            fillApiRequests(result.getSummary().getApiRequests(), result.getTrends().getApiRequests(), dashboardQuery, dt.needApiSummary(), dt.needApiTrend());
        }
        if (dt.needTops()) {
            fillTops(result.getTops(), runningTasks, latestSamples, dashboardQuery.top);
        }

        return result;
    }

    private TaskDashboardVo initResult(DashboardQuery dashboardQuery) {
        TaskDashboardVo result = new TaskDashboardVo();
        TaskDashboardVo.QueryInfo query = new TaskDashboardVo.QueryInfo();
        query.setType(dashboardQuery.type);
        query.setStep(dashboardQuery.step);
        query.setDashboardType(dashboardQuery.dashboardTypeEnum.value());
        query.setTop(dashboardQuery.top);
        query.setStartAt(dashboardQuery.startAtSec);
        query.setEndAt(dashboardQuery.endAtSec);
        result.setQuery(query);

        TaskDashboardVo.Summary summary = new TaskDashboardVo.Summary();
        summary.setActiveTasks(new TaskDashboardVo.ActiveTasks());
        summary.setTotalThroughput(new TaskDashboardVo.TotalThroughput());
        summary.setConnectedDbs(new TaskDashboardVo.ConnectedDbs());
        summary.setApiRequests(new TaskDashboardVo.ApiRequests());
        result.setSummary(summary);

        TaskDashboardVo.Trends trends = new TaskDashboardVo.Trends();
        trends.setThroughput(new TaskDashboardVo.Trend());
        trends.setApiRequests(new TaskDashboardVo.Trend());
        result.setTrends(trends);

        result.setTops(new TaskDashboardVo.Tops());
        return result;
    }

    private void fillActiveTasks(TaskDashboardVo.ActiveTasks activeTasks, List<TaskDto> tasks, Map<String, Sample> latestSamples) {
        activeTasks.setTotal(tasks.size());
        activeTasks.setRunning((int) tasks.stream().filter(task -> TaskDto.STATUS_RUNNING.equals(task.getStatus())).count());
        activeTasks.setError((int) tasks.stream()
                .filter(Objects::nonNull)
                .map(TaskDto::getStatus)
                .filter(status -> TaskDto.STATUS_ERROR.equals(status) || TaskDto.STATUS_SCHEDULE_FAILED.equals(status))
                .count());

        List<Long> lags = latestSamples.values().stream()
                .map(this::lagValue)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(lags)) {
            activeTasks.setMaxLag(lags.stream().max(Long::compareTo).orElse(0L));
            activeTasks.setMinLag(lags.stream().min(Long::compareTo).orElse(0L));
        }
    }

    private void fillThroughput(TaskDashboardVo.TotalThroughput totalThroughput, TaskDashboardVo.Trend trend, TaskMetricsTrendVo trendVo,
                                boolean includeSummary, boolean includeTrend) {
        List<Double> outputQps = Optional.ofNullable(trendVo.getOutputQps()).orElseGet(ArrayList::new);
        List<Double> outputSizeQps = Optional.ofNullable(trendVo.getOutputSizeQps()).orElseGet(ArrayList::new);
        if (includeTrend) {
            trend.setTs(Optional.ofNullable(trendVo.getTs()).orElseGet(ArrayList::new));
            trend.setValues(outputQps);
        }
        if (!includeSummary || CollectionUtils.isEmpty(outputQps)) {
            return;
        }

        double first = Optional.ofNullable(outputQps.get(0)).orElse(0D);
        double current = Optional.ofNullable(outputQps.get(outputQps.size() - 1)).orElse(0D);
        totalThroughput.setCurrent(current);
        totalThroughput.setPeak(outputQps.stream().filter(Objects::nonNull).max(Double::compareTo).orElse(0D));
        totalThroughput.setDataRate(CollectionUtils.isEmpty(outputSizeQps) ? 0D : Optional.ofNullable(outputSizeQps.get(outputSizeQps.size() - 1)).orElse(0D));
        totalThroughput.setChangeRate(first > 0D ? ((current - first) / first) * 100D : 0D);
    }

    private void fillConnectedDbs(TaskDashboardVo.ConnectedDbs connectedDbs, UserDetail user) {
        Query query = Query.query(Criteria.where("is_deleted").ne(true));
        query.fields().include("_id", "name", "tableCount");
        List<DataSourceConnectionDto> connections = Optional.ofNullable(dataSourceService.findAllDto(query, user)).orElseGet(ArrayList::new);
        connections.sort(Comparator
                .comparingLong((DataSourceConnectionDto dto) -> Optional.ofNullable(dto.getTableCount()).orElse(0L)).reversed()
                .thenComparing(dto -> StringUtils.defaultString(dto.getName())));
        connectedDbs.setTotal(connections.size());
        connectedDbs.setItems(connections.stream().map(dto -> {
            TaskDashboardVo.DbItem item = new TaskDashboardVo.DbItem();
            item.setId(Optional.ofNullable(dto.getId()).map(Object::toString).orElse(null));
            item.setName(dto.getName());
            item.setTableCount(Optional.ofNullable(dto.getTableCount()).orElse(0L));
            return item;
        }).limit(CONNECTED_DB_LIMIT).collect(Collectors.toList()));
    }

    private void fillApiRequests(TaskDashboardVo.ApiRequests apiRequests, TaskDashboardVo.Trend trend, DashboardQuery dashboardQuery,
                                 boolean includeSummary, boolean includeTrend) {
        if (includeSummary) {
            QueryBase summaryQuery = buildApiQuery(dashboardQuery);
            ServerTopOnHomepage summary = Optional.ofNullable(apiMetricsChartQuery.serverTopOnHomepage(summaryQuery)).orElse(ServerTopOnHomepage.create());
            apiRequests.setTotal(Optional.ofNullable(summary.getTotalRequestCount()).orElse(0L));
            apiRequests.setFailed(Optional.ofNullable(summary.getErrorCount()).orElse(0L));
            apiRequests.setErrorRate(Optional.ofNullable(summary.getTotalErrorRate()).orElse(0D));
            apiRequests.setAvgTime(Optional.ofNullable(summary.getResponseTimeAvg()).orElse(0D));
        }

        if (includeTrend) {
            QueryBase trendQuery = buildApiQuery(dashboardQuery);
            ApiRequestTrend apiRequestTrend = Optional.ofNullable(apiMetricsChartQuery.homepageRequestTrend(trendQuery))
                    .orElse(ApiRequestTrend.create(TimeGranularity.SECOND_FIVE));
            trend.setTs(apiRequestTrend.getTs());
            trend.setValues(apiRequestTrend.getValues());
        }
    }

    private QueryBase buildApiQuery(DashboardQuery dashboardQuery) {
        QueryBase queryBase = new QueryBase();
        queryBase.setType(dashboardQuery.type);
        queryBase.setStep(dashboardQuery.step);
        queryBase.setStartAt(dashboardQuery.startAtSec);
        queryBase.setEndAt(dashboardQuery.endAtSec);
        return queryBase;
    }

    private void fillTops(TaskDashboardVo.Tops tops, List<TaskDto> runningTasks, Map<String, Sample> latestSamples, int topLimit) {
        List<TaskDashboardVo.TaskRankItem> items = runningTasks.stream()
                .map(task -> rankItem(task, latestSamples.get(task.getId().toHexString())))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        tops.setTopLaggingTasks(items.stream()
                .sorted(Comparator.comparingLong(TaskDashboardVo.TaskRankItem::getLatency).reversed()
                        .thenComparing(TaskDashboardVo.TaskRankItem::getThroughput, Comparator.reverseOrder()))
                .limit(topLimit)
                .collect(Collectors.toList()));

        tops.setTopThroughputTasks(items.stream()
                .sorted(Comparator.comparingDouble(TaskDashboardVo.TaskRankItem::getThroughput).reversed()
                        .thenComparing(TaskDashboardVo.TaskRankItem::getLatency, Comparator.reverseOrder()))
                .limit(topLimit)
                .collect(Collectors.toList()));
    }

    private TaskDashboardVo.TaskRankItem rankItem(TaskDto task, Sample sample) {
        if (sample == null) {
            return null;
        }
        TaskDashboardVo.TaskRankItem item = new TaskDashboardVo.TaskRankItem();
        item.setTaskId(task.getId().toHexString());
        item.setTaskName(task.getName());
        item.setLatency(Optional.ofNullable(lagValue(sample)).orElse(0L));
        item.setThroughput(Optional.ofNullable(outputQpsValue(sample)).orElse(0D));
        return item;
    }

    private Long lagValue(Sample sample) {
        return Optional.ofNullable(sample)
                .map(Sample::getVs)
                .map(vs -> vs.get("replicateLag"))
                .map(Number::longValue)
                .orElse(null);
    }

    private Double outputQpsValue(Sample sample) {
        return Optional.ofNullable(sample)
                .map(Sample::getVs)
                .map(vs -> vs.get("outputQps"))
                .map(Number::doubleValue)
                .orElse(null);
    }

    private DashboardQuery normalize(String type, Long step, String dashboardType, Integer top) {
        DashboardQuery query = new DashboardQuery();
        query.type = DEFAULT_TYPE;
        query.step = DEFAULT_STEP;
        query.dashboardTypeEnum = DashboardType.fromValue(dashboardType);
        query.top = normalizeTop(top);
        if ("hours".equalsIgnoreCase(type) && Long.valueOf(1L).equals(step)) {
            query.type = "hours";
            query.step = 1L;
        } else if ("days".equalsIgnoreCase(type) && Long.valueOf(1L).equals(step)) {
            query.type = "days";
            query.step = 1L;
        }
        query.endAtSec = Instant.now().getEpochSecond();
        long rangeSeconds = switch (query.type) {
            case "hours" -> 60L * 60L;
            case "days" -> 24L * 60L * 60L;
            default -> 5L * 60L;
        };
        query.startAtSec = query.endAtSec - rangeSeconds;
        query.startAtMs = query.startAtSec * 1000L;
        query.endAtMs = query.endAtSec * 1000L;
        return query;
    }

    private int normalizeTop(Integer top) {
        if (top == null) {
            return DEFAULT_TOP_LIMIT;
        }
        return switch (top) {
            case 5, 10, 20 -> top;
            default -> DEFAULT_TOP_LIMIT;
        };
    }

    private static class DashboardQuery {
        private String type;
        private long step;
        private DashboardType dashboardTypeEnum;
        private int top;
        private long startAtSec;
        private long endAtSec;
        private long startAtMs;
        private long endAtMs;
    }
}
