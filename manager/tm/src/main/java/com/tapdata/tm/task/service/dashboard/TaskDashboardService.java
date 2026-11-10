package com.tapdata.tm.task.service.dashboard;

import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.cluster.dto.AccessNodeInfo;
import com.tapdata.tm.cluster.dto.Component;
import com.tapdata.tm.cluster.dto.ClusterStateDto;
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
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
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
    private static final long MAX_TREND_RANGE_SECONDS = 7L * 24L * 60L * 60L;

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
    private ClusterStateService clusterStateService;
    private Clock clock = Clock.systemUTC();

    @Autowired(required = false)
    public void setClock(Clock clock) {
        if (clock != null) {
            this.clock = clock;
        }
    }

    public TaskDashboardVo dashboard(UserDetail user, String type, Long step, String dashboardType, Integer top) {
        return dashboard(user, type, step, dashboardType, top, null, null, null, null);
    }

    public TaskDashboardVo dashboard(UserDetail user, String type, Long step, String dashboardType, Integer top,
                                     String physicalNodeId, Long startAt, Long endAt, Integer trendSchemaVersion) {
        DashboardQuery dashboardQuery = normalize(type, step, dashboardType, top, physicalNodeId, startAt, endAt, trendSchemaVersion);
        DashboardType dt = dashboardQuery.dashboardTypeEnum;
        TaskDashboardVo result = initResult(dashboardQuery);
        TaskDashboardVo.NodeOption selectedNode = dashboardQuery.version2 && StringUtils.isNotBlank(physicalNodeId)
                ? findAuthorizedNode(user, dashboardQuery.physicalNodeId)
                : null;

        boolean needTaskList = dt.needActiveTasks() || dt.needTops() || dt.needThroughput();
        boolean needLatestSamples = dt.needActiveTasks() || dt.needTops();
        boolean needTaskTrend = dt.needThroughput();

        List<TaskDto> tasks = needTaskList
                ? Optional.ofNullable(chartViewService.getViewTaskDtoByUser(user)).orElseGet(ArrayList::new)
                : new ArrayList<>();

        // 查询系统连接 ID（MDM/FDM），用于过滤系统任务
        Set<String> systemConnectionIds = needTaskList ? findSystemConnectionIds() : Set.of();
        List<TaskDto> runningTasks = tasks.stream()
                .filter(task -> TaskDto.STATUS_RUNNING.equals(task.getStatus()))
                .collect(Collectors.toList());
        List<String> runningTaskIds = runningTasks.stream()
                .map(TaskDto::getId)
                .filter(Objects::nonNull)
                .map(id -> id.toHexString())
                .collect(Collectors.toList());
        List<TaskDto> filterTasks = runningTasks.stream()
                .filter(task -> !isSystemTask(task, systemConnectionIds))
                .collect(Collectors.toList());
        List<TaskDto> trendTasks = dashboardQuery.version2
                ? tasks.stream().filter(task -> !isSystemTask(task, systemConnectionIds)).collect(Collectors.toList())
                : filterTasks;
        List<String> trendTaskIds = trendTasks.stream()
                .map(TaskDto::getId)
                .filter(Objects::nonNull)
                .map(id -> id.toHexString())
                .collect(Collectors.toList());

        Map<String, Sample> latestSamples = needLatestSamples && CollectionUtils.isNotEmpty(runningTasks)
                ? Optional.ofNullable(measurementServiceV2.findLastMinuteSamplesByTaskIds(runningTaskIds)).orElseGet(HashMap::new)
                : new HashMap<>();
        TaskMetricsTrendVo trendVo = new TaskMetricsTrendVo();
        if (needTaskTrend && CollectionUtils.isNotEmpty(trendTasks)) {
            if (dashboardQuery.version2) {
                trendVo = measurementServiceV2.aggregateTaskMetricsByTaskIdsV2(
                        trendTaskIds, dashboardQuery.startAtMs, dashboardQuery.endAtMs, dashboardQuery.physicalNodeId);
                if (trendVo == null) {
                    throw new IllegalStateException("Task Metrics V2 query returned no trend object");
                }
            } else {
                trendVo = Optional.ofNullable(measurementServiceV2.aggregateTaskMetricsByTaskIds(
                        trendTaskIds, dashboardQuery.startAtMs, dashboardQuery.endAtMs)).orElseGet(TaskMetricsTrendVo::new);
            }
        }

        if (dt.needActiveTasks()) {
            fillActiveTasks(result.getSummary().getActiveTasks(), tasks, latestSamples);
        }
        if (dt.needThroughput()) {
            if (dashboardQuery.version2) {
                fillThroughputV2(result.getTrends().getThroughput(), trendVo, dashboardQuery, selectedNode);
            } else {
                fillThroughput(result.getSummary().getTotalThroughput(), result.getTrends().getThroughput(), trendVo, dashboardQuery, dt.needThroughputSummary(), dt.needThroughputTrend());
            }
        }
        if (dt.needConnectedDbs()) {
            fillConnectedDbs(result.getSummary().getConnectedDbs(), user);
        }
        if (dt.needApiRequests()) {
            if (dashboardQuery.version2) {
                fillApiRequestsV2(result.getTrends().getApiRequests(), dashboardQuery, selectedNode);
            } else {
                fillApiRequests(result.getSummary().getApiRequests(), result.getTrends().getApiRequests(), dashboardQuery, dt.needApiSummary(), dt.needApiTrend());
            }
        }
        if (dt.needTops()) {
            fillTops(result.getTops(), runningTasks, latestSamples, dashboardQuery.top);
        }

        return result;
    }


    private Set<String> findSystemConnectionIds() {
        Query sourceQuery = Query.query(Criteria.where("name").in("MDM", "FDM"));
        sourceQuery.fields().include("_id");
        List<DataSourceConnectionDto> connectionDtos = dataSourceService.findAll(sourceQuery);
        return connectionDtos.stream()
                .map(dto -> dto.getId().toHexString())
                .collect(Collectors.toCollection(HashSet::new));
    }

    private boolean isSystemTask(TaskDto taskDto, Set<String> systemConnectionIds) {
        if (systemConnectionIds.isEmpty() || null == taskDto.getDag()) {
            return false;
        }
        List<Node> sources = taskDto.getDag().getSources();
        if (CollectionUtils.isEmpty(sources)) {
            return false;
        }
        return sources.stream()
                .filter(node -> node instanceof DataParentNode)
                .map(node -> ((DataParentNode<?>) node).getConnectionId())
                .anyMatch(systemConnectionIds::contains);
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
        query.setPhysicalNodeId(dashboardQuery.physicalNodeId);
        query.setIntervalSeconds(dashboardQuery.intervalSeconds);
        query.setTrendSchemaVersion(dashboardQuery.trendSchemaVersion);
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

    private void fillThroughput(TaskDashboardVo.TotalThroughput totalThroughput, TaskDashboardVo.Trend trend, TaskMetricsTrendVo trendVo, DashboardQuery dashboardQuery,
                                boolean includeSummary, boolean includeTrend) {
        List<Double> outputQps = Optional.ofNullable(trendVo.getOutputQps()).orElseGet(ArrayList::new);
        List<Double> outputSizeQps = Optional.ofNullable(trendVo.getOutputSizeQps()).orElseGet(ArrayList::new);
        if (includeTrend) {
            List<Long> ts = Optional.ofNullable(trendVo.getTs()).orElseGet(ArrayList::new);
            if (CollectionUtils.isEmpty(ts) || CollectionUtils.isEmpty(outputQps)) {
                fillZeroTrend(trend, buildThroughputTimeline(dashboardQuery));
            } else {
                trend.setTs(ts);
                trend.setValues(outputQps);
            }
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

    private void fillThroughputV2(TaskDashboardVo.Trend trend, TaskMetricsTrendVo trendVo,
                                  DashboardQuery dashboardQuery, TaskDashboardVo.NodeOption selectedNode) {
        List<Long> timeline = buildV2Timeline(dashboardQuery);
        List<Double> values = trendVo.getOutputQps();
        if (trendVo.getTs().isEmpty()) {
            values = new ArrayList<>(Collections.nCopies(timeline.size(), null));
        } else if (!timeline.equals(trendVo.getTs()) || values.size() != timeline.size()) {
            throw new IllegalStateException("Task throughput trend buckets do not match the requested dashboard window");
        }
        String status = Optional.ofNullable(trendVo.getStatus()).orElse("no_data");
        if (selectedNode != null && !selectedNode.isEngineDeployed()
                && !trendVo.isHasSelectedNodeData() && !trendVo.isHasUnattributedData()) {
            status = "component_not_deployed";
            values = new ArrayList<>(Collections.nCopies(timeline.size(), null));
        }
        trend.setTs(timeline);
        trend.setValues(values);
        trend.setStatus(status);
        trend.setTraceability(Optional.ofNullable(trendVo.getTraceability()).orElse("complete"));
        trend.setCoverage(Optional.ofNullable(trendVo.getCoverage()).orElse("unknown"));
        trend.setHasUnattributedData(trendVo.isHasUnattributedData());
    }

    private void fillConnectedDbs(TaskDashboardVo.ConnectedDbs connectedDbs, UserDetail user) {
        Query query = Query.query(Criteria.where("is_deleted").ne(true));
        query.fields().include("_id", "name", "tableCount");
        List<DataSourceConnectionDto> connections = new ArrayList<>(Optional.ofNullable(dataSourceService.findAllDto(query, user)).orElseGet(ArrayList::new));
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
            List<Long> ts = Optional.ofNullable(apiRequestTrend.getTs()).orElseGet(ArrayList::new);
            List<Double> values = Optional.ofNullable(apiRequestTrend.getValues()).orElseGet(ArrayList::new);
            if (CollectionUtils.isEmpty(ts) || CollectionUtils.isEmpty(values)) {
                fillZeroTrend(trend, buildApiRequestTimeline(dashboardQuery));
            } else {
                trend.setTs(ts);
                trend.setValues(values);
            }
        }
    }

    private void fillApiRequestsV2(TaskDashboardVo.Trend trend, DashboardQuery dashboardQuery,
                                   TaskDashboardVo.NodeOption selectedNode) {
        ApiRequestTrend apiRequestTrend = apiMetricsChartQuery.homepageRequestTrendV2(
                buildApiTrendQueryV2(dashboardQuery), dashboardQuery.physicalNodeId);
        String status = Optional.ofNullable(apiRequestTrend.getStatus()).orElse("no_data");
        List<Double> values = apiRequestTrend.getValues();
        if (selectedNode != null && !selectedNode.isApiServerDeployed()
                && "no_data".equals(status) && !apiRequestTrend.isHasUnattributedData()) {
            status = "component_not_deployed";
            values = new ArrayList<>(Collections.nCopies(apiRequestTrend.getTs().size(), null));
        }
        trend.setTs(apiRequestTrend.getTs());
        trend.setValues(values);
        trend.setStatus(status);
        trend.setTraceability(Optional.ofNullable(apiRequestTrend.getTraceability()).orElse("complete"));
        trend.setCoverage(Optional.ofNullable(apiRequestTrend.getCoverage()).orElse("unknown"));
        trend.setHasUnattributedData(apiRequestTrend.isHasUnattributedData());
    }

    private void fillZeroTrend(TaskDashboardVo.Trend trend, List<Long> ts) {
        trend.setTs(ts);
        trend.setValues(ts.stream().map(value -> 0D).collect(Collectors.toList()));
    }

    private List<Long> buildThroughputTimeline(DashboardQuery dashboardQuery) {
        long rangeMs = dashboardQuery.endAtMs - dashboardQuery.startAtMs;
        long stepSeconds;
        if (rangeMs <= 5L * 60L * 1000L) {
            stepSeconds = 5L;
        } else if (rangeMs <= 60L * 60L * 1000L) {
            stepSeconds = 60L;
        } else if (rangeMs <= 24L * 60L * 60L * 1000L) {
            stepSeconds = 60L * 60L;
        } else {
            stepSeconds = 24L * 60L * 60L;
        }
        return buildTimeline(dashboardQuery.startAtSec, dashboardQuery.endAtSec, stepSeconds);
    }

    private List<Long> buildApiRequestTimeline(DashboardQuery dashboardQuery) {
        long rangeSeconds = dashboardQuery.endAtSec - dashboardQuery.startAtSec;
        TimeGranularity granularity;
        if (rangeSeconds < TimeGranularity.HOUR.getSeconds()) {
            granularity = TimeGranularity.SECOND_FIVE;
        } else if (rangeSeconds < TimeGranularity.DAY.getSeconds()) {
            granularity = TimeGranularity.MINUTE;
        } else {
            granularity = TimeGranularity.HOUR;
        }
        return buildTimeline(dashboardQuery.startAtSec, dashboardQuery.endAtSec, granularity.getSeconds());
    }

    private List<Long> buildTimeline(long startAtSec, long endAtSec, long stepSeconds) {
        if (startAtSec > endAtSec || stepSeconds <= 0L) {
            return new ArrayList<>();
        }
        long start = startAtSec / stepSeconds * stepSeconds;
        long end = endAtSec / stepSeconds * stepSeconds;
        List<Long> ts = new ArrayList<>();
        for (long cursor = start; cursor <= end; cursor += stepSeconds) {
            ts.add(cursor);
        }
        return ts;
    }

    private QueryBase buildApiQuery(DashboardQuery dashboardQuery) {
        QueryBase queryBase = new QueryBase();
        queryBase.setType(dashboardQuery.type);
        queryBase.setStep(dashboardQuery.step);
        queryBase.setStartAt(dashboardQuery.startAtSec);
        queryBase.setEndAt(dashboardQuery.endAtSec);
        return queryBase;
    }

    private QueryBase buildApiTrendQueryV2(DashboardQuery dashboardQuery) {
        QueryBase queryBase = new QueryBase();
        queryBase.setType("range");
        queryBase.setStartAt(dashboardQuery.startAtSec);
        queryBase.setEndAt(dashboardQuery.endAtSec);
        queryBase.setPhysicalNodeId(dashboardQuery.physicalNodeId);
        queryBase.setGranularity(dashboardQuery.intervalSeconds <= 5L ? TimeGranularity.SECOND_FIVE
                : dashboardQuery.intervalSeconds <= 60L ? TimeGranularity.MINUTE : TimeGranularity.HOUR);
        return queryBase;
    }

    private List<Long> buildV2Timeline(DashboardQuery dashboardQuery) {
        List<Long> timeline = new ArrayList<>();
        long start = dashboardQuery.startAtSec - dashboardQuery.startAtSec % dashboardQuery.intervalSeconds;
        for (long cursor = start; cursor < dashboardQuery.endAtSec; cursor += dashboardQuery.intervalSeconds) {
            timeline.add(cursor);
        }
        return timeline;
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

    private DashboardQuery normalize(String type, Long step, String dashboardType, Integer top,
                                     String physicalNodeId, Long startAt, Long endAt, Integer trendSchemaVersion) {
        DashboardQuery query = new DashboardQuery();
        query.type = DEFAULT_TYPE;
        query.step = DEFAULT_STEP;
        query.dashboardTypeEnum = DashboardType.fromValue(dashboardType);
        query.top = normalizeTop(top);
        if ("hours".equalsIgnoreCase(type)) {
            query.type = "hours";
            query.step = 1L;
        } else if ("days".equalsIgnoreCase(type)) {
            query.type = "days";
            query.step = 1L;
        }
        boolean hasV2Arguments = StringUtils.isNotBlank(physicalNodeId) || startAt != null || endAt != null
                || trendSchemaVersion != null || "range".equalsIgnoreCase(type);
        if (hasV2Arguments && !Integer.valueOf(2).equals(trendSchemaVersion)) {
            throw invalidDashboardQuery();
        }
        query.version2 = Integer.valueOf(2).equals(trendSchemaVersion);
        if (query.version2 && query.dashboardTypeEnum != DashboardType.TRENDS) {
            throw invalidDashboardQuery();
        }

        long now = clock.instant().getEpochSecond();
        if (query.version2) {
            query.physicalNodeId = StringUtils.trimToNull(physicalNodeId);
            query.trendSchemaVersion = 2;
            if ((startAt == null) != (endAt == null)) {
                throw invalidDashboardQuery();
            }
            if (startAt == null) {
                if ("range".equalsIgnoreCase(type)) {
                    throw invalidDashboardQuery();
                }
                query.endAtSec = now;
                query.startAtSec = now - presetRangeSeconds(query.type);
            } else {
                if (startAt <= 0L || startAt >= endAt || endAt > now) {
                    throw invalidDashboardQuery();
                }
                if (endAt - startAt > MAX_TREND_RANGE_SECONDS) {
                    throw new BizException("query.range.too.large", 7);
                }
                query.type = "range";
                query.startAtSec = startAt;
                query.endAtSec = endAt;
            }
            long rangeSeconds = query.endAtSec - query.startAtSec;
            query.intervalSeconds = rangeSeconds <= 5L * 60L ? 5L : rangeSeconds <= 60L * 60L ? 60L : 3600L;
            query.step = query.intervalSeconds;
        } else {
            query.endAtSec = now;
            query.startAtSec = query.endAtSec - presetRangeSeconds(query.type);
        }
        query.startAtMs = query.startAtSec * 1000L;
        query.endAtMs = query.endAtSec * 1000L;
        return query;
    }

    private long presetRangeSeconds(String type) {
        return switch (type) {
            case "hours" -> 60L * 60L;
            case "days" -> 24L * 60L * 60L;
            default -> 5L * 60L;
        };
    }

    private BizException invalidDashboardQuery() {
        return new BizException("task.dashboard.query.invalid");
    }

    private TaskDashboardVo.NodeOption findAuthorizedNode(UserDetail user, String physicalNodeId) {
        return dashboardNodes(user, physicalNodeId).stream()
                .filter(node -> physicalNodeId.equals(node.getPhysicalNodeId()))
                .findFirst()
                .orElseThrow(() -> new BizException("server.not.exists"));
    }

    public List<TaskDashboardVo.NodeOption> dashboardNodes(UserDetail user) {
        return dashboardNodes(user, null);
    }

    private List<TaskDashboardVo.NodeOption> dashboardNodes(UserDetail user, String physicalNodeId) {
        Map<String, String> processStatuses = new HashMap<>();
        List<AccessNodeInfo> accessNodes = clusterStateService.findAccessNodeInfo(user);
        if (CollectionUtils.isEmpty(accessNodes)) {
            return new ArrayList<>();
        }
        accessNodes.forEach(node -> collectAccessNodeStatuses(node, processStatuses));
        if (processStatuses.isEmpty()) {
            return new ArrayList<>();
        }

        Criteria accessCriteria = Criteria.where("systemInfo.process_id").in(processStatuses.keySet());
        if (StringUtils.isNotBlank(physicalNodeId)) {
            accessCriteria = new Criteria().andOperator(accessCriteria, new Criteria().orOperator(
                    Criteria.where("uuid").is(physicalNodeId),
                    Criteria.where("systemInfo.uuid").is(physicalNodeId)));
        }
        Query query = Query.query(accessCriteria);
        query.fields().include("uuid", "agentName", "systemInfo", "engine", "apiServer");
        List<ClusterStateDto> states = clusterStateService.findAll(query);
        Map<String, TaskDashboardVo.NodeOption> optionsByNodeId = new HashMap<>();
        for (ClusterStateDto state : states) {
            String physicalId = Optional.ofNullable(state.getUuid())
                    .orElseGet(() -> Optional.ofNullable(state.getSystemInfo()).map(info -> info.getUuid()).orElse(null));
            if (StringUtils.isBlank(physicalId)) {
                continue;
            }
            String processId = Optional.ofNullable(state.getSystemInfo()).map(info -> info.getProcess_id()).orElse(null);
            String processStatus = processStatuses.getOrDefault(processId, "offline");
            String host = Optional.ofNullable(state.getAgentName())
                    .filter(StringUtils::isNotBlank)
                    .orElseGet(() -> Optional.ofNullable(state.getSystemInfo())
                            .map(info -> info.getHostname())
                            .filter(StringUtils::isNotBlank)
                            .orElse("Node"));
            String suffix = physicalId.substring(Math.max(0, physicalId.length() - 6));
            TaskDashboardVo.NodeOption option = optionsByNodeId.computeIfAbsent(physicalId, id -> {
                TaskDashboardVo.NodeOption newOption = new TaskDashboardVo.NodeOption();
                newOption.setPhysicalNodeId(id);
                newOption.setLabel(host + " (" + suffix + ")");
                newOption.setStatus("running".equalsIgnoreCase(processStatus) ? "running" : "offline");
                return newOption;
            });
            option.setEngineDeployed(option.isEngineDeployed() || isComponentDeployed(state.getEngine()));
            option.setApiServerDeployed(option.isApiServerDeployed() || isComponentDeployed(state.getApiServer()));
            if ("running".equalsIgnoreCase(processStatus)) {
                option.setStatus("running");
            }
        }
        return optionsByNodeId.values().stream()
                .sorted(Comparator.comparing(TaskDashboardVo.NodeOption::getLabel, String.CASE_INSENSITIVE_ORDER))
                .collect(Collectors.toList());
    }

    private boolean isComponentDeployed(Component component) {
        return component != null && StringUtils.isNotBlank(component.getStatus())
                && !"not_deploy".equalsIgnoreCase(component.getStatus());
    }

    private void collectAccessNodeStatuses(AccessNodeInfo node, Map<String, String> statuses) {
        if (node == null) {
            return;
        }
        if (CollectionUtils.isNotEmpty(node.getAccessNodes())) {
            node.getAccessNodes().forEach(child -> collectAccessNodeStatuses(child, statuses));
            return;
        }
        if (StringUtils.isNotBlank(node.getProcessId())) {
            statuses.put(node.getProcessId(), node.getStatus());
        }
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
        private String physicalNodeId;
        private long intervalSeconds;
        private Integer trendSchemaVersion;
        private boolean version2;
    }
}
