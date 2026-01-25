package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.cluster.dto.Component;
import com.tapdata.tm.cluster.entity.ClusterStateEntity;
import com.tapdata.tm.cluster.repository.ClusterStateRepository;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.utils.ApiMetricsDelayUtil;
import com.tapdata.tm.utils.MongoUtils;
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
import com.tapdata.tm.v2.api.monitor.main.param.TopWorkerInServerParam;
import com.tapdata.tm.v2.api.monitor.utils.ApiMetricsDelayInfoUtil;
import com.tapdata.tm.v2.api.monitor.utils.ApiPathUtil;
import com.tapdata.tm.v2.api.monitor.utils.ChartSortUtil;
import com.tapdata.tm.v2.api.monitor.utils.TimeRangeUtil;
import com.tapdata.tm.v2.api.usage.repository.ServerUsageMetricRepository;
import com.tapdata.tm.v2.api.usage.repository.UsageRepository;
import com.tapdata.tm.worker.dto.ApiServerStatus;
import com.tapdata.tm.worker.dto.ApiServerWorkerInfo;
import com.tapdata.tm.worker.dto.MetricInfo;
import com.tapdata.tm.worker.entity.ServerUsage;
import com.tapdata.tm.worker.entity.UsageBase;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.repository.WorkerRepository;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/31 09:15 Create
 * @description
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class ApiMetricsChartQuery {
    public static final String WORKER_TYPE = "worker_type";
    public static final String PROCESS_ID = "processId";
    public static final String PROCESS_TYPE = "processType";
    public static final String API_SERVER = "api-server";
    public static final String SERVER_ID_EMPTY = "server.id.empty";
    ApiMetricsRawService service;
    UsageRepository usageRepository;
    WorkerRepository workerRepository;
    ClusterStateRepository clusterRepository;
    ModulesService modulesService;
    ServerUsageMetricRepository serverUsageMetricRepository;
    ApiMetricsRawMergeService metricsRawMergeService;

    public ApiMetricsChartQuery() {

    }

    public ServerTopOnHomepage serverTopOnHomepage(QueryBase param) {
        final ServerTopOnHomepage result = ServerTopOnHomepage.create();
        long delay = metricsRawMergeService.getDelay();
        TimeRangeUtil.rangeOf(result, param, delay, true);
        List<ApiMetricsRaw> apiMetricsRaws = metricsRawMergeService.merge(
                param,
                c -> c.and("metricType").is(MetricTypes.ALL.getType()),
                null,
                new String[]{"apiId", "processId", "timeGranularity", "timeStart", "reqCount", "errorCount", "delay"}
        );
        if (CollectionUtils.isEmpty(apiMetricsRaws)) {
            return result;
        }
        final AtomicLong errorCount = new AtomicLong(0L);
        for (final ApiMetricsRaw e : apiMetricsRaws) {
            if (null == e) {
                continue;
            }
            Optional.ofNullable(e.getReqCount()).ifPresent(v -> result.setTotalRequestCount(result.getTotalRequestCount() + v));
            Optional.ofNullable(e.getErrorCount()).ifPresent(errorCount::addAndGet);
        }
        final Long totalRequestCount = result.getTotalRequestCount();
        if (totalRequestCount > 0L) {
            long errorNum = errorCount.get();
            if (errorNum > 0L) {
                Long erroredCount = metricsRawMergeService.errorCount(
                        param, c -> c.and("metricType").is(MetricTypes.API.getType()),
                        null);
                result.setNotHealthyApiCount(Optional.ofNullable(erroredCount).orElse(0L));
                result.setErrorCount(errorNum);
            }
            result.setTotalErrorRate(ApiMetricsDelayInfoUtil.rate(errorNum, totalRequestCount));
            metricsRawMergeService.baseDataCalculate(result, apiMetricsRaws, result::setResponseTime);
        }
        return result;
    }

    public List<ServerItem> serverOverviewList(ServerListParam param) {
        List<ServerItem> result = new ArrayList<>();
        long delay = metricsRawMergeService.getDelay();
        TimeRangeUtil.rangeOf(param, delay, true);
        String serverMatchName = param.getServerName();
        //find all server
        Criteria criteriaOfWorker = Criteria.where(WORKER_TYPE).is(API_SERVER)
                .and("delete").ne(true);
        if (StringUtils.isNotBlank(serverMatchName)) {
            serverMatchName = serverMatchName.trim();
            criteriaOfWorker.and("hostname").regex(serverMatchName);
        }
        final Query queryOfWorker = Query.query(criteriaOfWorker);
        queryOfWorker.fields().include(PROCESS_ID, "workerStatus", "pingTime", "deleted", "hostname");
        List<Worker> serverInfos = workerRepository.findAll(queryOfWorker);
        Map<String, Worker> serverMap = serverInfos.stream()
                .collect(Collectors.toMap(Worker::getProcessId, e -> e, (e1, e2) -> e2));
        if (CollectionUtils.isEmpty(serverInfos)) {
            return ServerItem.supplement(result, activeWorkers(null));
        }
        boolean filterServer = StringUtils.isNotBlank(serverMatchName);
        Criteria apiCallCriteria = null;
        if (filterServer) {
            apiCallCriteria = Criteria.where("api_gateway_uuid").in(serverMap.keySet());
        }
        List<ApiMetricsRaw> apiMetricsRaws = metricsRawMergeService.merge(
                param,
                c -> {
                    c.and("metricType").is(MetricTypes.SERER.getType());
                    if (filterServer) {
                        c.and(PROCESS_ID).in(serverMap.keySet());
                    }
                },
                apiCallCriteria,
                new String[]{"apiId", "processId", "timeGranularity", "timeStart", "reqCount", "errorCount", "delay"});
        //find cluster state of worker
        Criteria criteriaOfCluster = Criteria.where("apiServer.serverId").in(serverMap.keySet());
        Query queryOfCluster = Query.query(criteriaOfCluster);
        queryOfCluster.fields().include("apiServer");
        List<ClusterStateEntity> clusterStateEntities = clusterRepository.findAll(queryOfCluster);
        Map<String, Component> clusterStateOfWorker = clusterStateEntities.stream()
                .filter(Objects::nonNull)
                .filter(e -> Objects.nonNull(e.getApiServer()) && Objects.nonNull(e.getApiServer().getServerId()))
                .collect(Collectors.toMap(e -> e.getApiServer().getServerId(), ClusterStateEntity::getApiServer, (e1, e2) -> e2));

        //find all server's usage info
        Criteria criteriaOfUsage = Criteria.where(PROCESS_ID).in(serverMap.keySet())
                .and(PROCESS_TYPE).is(ServerUsage.ProcessType.API_SERVER.getType());
        List<? extends UsageBase> allUsage = queryCpuUsageRecords(criteriaOfUsage, param.getRealStart(), param.getRealEnd(), param.getGranularity());
        Map<String, ServerChart.Usage> usageMap = allUsage.stream()
                .filter(Objects::nonNull)
                .filter(e -> StringUtils.isNotBlank(e.getProcessId()))
                .collect(Collectors.groupingBy(UsageBase::getProcessId, Collectors.collectingAndThen(
                        Collectors.toList(),
                        items -> this.mapUsage(items, param.getRealStart(), param.getRealEnd(), param.getGranularity())
                )));
        final Map<String, ServerItem> collect = apiMetricsRaws.stream()
                .filter(Objects::nonNull)
                .filter(e -> StringUtils.isNotBlank(e.getProcessId()))
                .collect(Collectors.groupingBy(
                        ApiMetricsRaw::getProcessId,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                infos -> {
                                    final ServerItem item = ServerItem.create();
                                    long errorCount = metricsRawMergeService.errorCountGetter(infos, e -> item.setRequestCount(item.getRequestCount() + e));
                                    long total = item.getRequestCount();
                                    if (total > 0L) {
                                        item.setErrorRate(ApiMetricsDelayInfoUtil.rate(errorCount, item.getRequestCount()));
                                        item.setErrorCount(errorCount);
                                        metricsRawMergeService.baseDataCalculate(item, infos, null);
                                    }
                                    final ApiMetricsRaw first = infos.get(0);
                                    final String processId = first.getProcessId();
                                    final Worker worker = serverMap.get(processId);
                                    asServerItemInfo(processId, item, usageMap, worker, clusterStateOfWorker, param);
                                    return item;
                                }
                        )
                ));
        serverMap.forEach((processId, worker) -> {
            if (!collect.containsKey(processId)) {
                final ServerItem item = ServerItem.create();
                asServerItemInfo(processId, item, usageMap, worker, clusterStateOfWorker, param);
                result.add(item);
                return;
            }
            result.add(collect.get(processId));
        });
        ServerItem.supplement(result, activeWorkers(serverMap.keySet()));
        result.sort(Comparator.comparing(ServerItem::getServerName));
        return result;
    }

    protected <T extends UsageBase> List<T> queryCpuUsageRecords(Criteria criteriaBase, long queryStart, long queryEnd, TimeGranularity type) {
        long start = queryStart * 1000L;
        long end = queryEnd * 1000L;
        if (type == TimeGranularity.SECOND_FIVE) {
            //往前推一个5秒点，防止数据上报不及时导致前端显示0
            long now = System.currentTimeMillis() / 5000L * 5000L - 5000L;
            if (end > now) {
                end = now;
            }
            criteriaBase.and("lastUpdateTime").gte(start).lt(end);
            criteriaBase.and("type").in(List.of(0, 1, 2));
            Query queryOfUsage = Query.query(criteriaBase);
            return (List<T>) usageRepository.findAll(queryOfUsage);
        } else if (type == TimeGranularity.MINUTE) {
            start = start / 60000L * 60000L;
        } else {
            start = start / 3600000L * 3600000L;
        }
        criteriaBase.and("lastUpdateTime").gte(start).lt(end);
        Query query = Query.query(criteriaBase);
        return (List<T>) serverUsageMetricRepository.findAll(query);
    }

    protected ServerChart.Usage mapUsage(List<? extends UsageBase> infos, long startAt, long endAt, TimeGranularity granularity) {
        final ServerChart.Usage usage = ServerChart.Usage.create();
        // Calculate step based on granularity
        long step = ApiMetricsDelayInfoUtil.stepByGranularity(granularity);
        if (infos.isEmpty()) {
            return usage;
        }
        // Sort by time
        infos.sort(Comparator.comparingLong(UsageBase::getLastUpdateTime));
        // Fill gaps before first data point
        endAt = granularity.fixTime(endAt);
        long currentTime = granularity.fixTime(startAt);
        long firstDataTime = infos.get(0).getLastUpdateTime() / 1000L;
        while (currentTime < firstDataTime) {
            //usage.addEmpty(currentTime, granularity != 0);
            currentTime += step;
        }
        // Process each data point
        long lastProcessedTime = currentTime;
        for (UsageBase info : infos) {
            long ts = info.getLastUpdateTime() / 1000L;
            // Fill any gaps between data points
            while (lastProcessedTime < ts) {
                usage.addEmpty(lastProcessedTime, granularity != TimeGranularity.SECOND_FIVE);
                lastProcessedTime += step;
            }
            // Add the actual data point if not already added
            if (lastProcessedTime == ts) {
                usage.add(info);
                lastProcessedTime += step;
            }
        }
        // Fill gaps after last data point until endAt
        long lastDataTime = infos.get(infos.size() - 1).getLastUpdateTime() / 1000L;
        long fillTime = lastDataTime + step;
        while (fillTime < endAt) {
            //usage.addEmpty(fillTime, granularity != 0);
            fillTime += step;
        }
        //fixUsage(usage.getTs(), usage.getCpuUsage(), usage.getMemoryUsage());
        return usage;
    }

    protected void asServerItemInfo(String processId, ServerItem item, Map<String, ServerChart.Usage> usageMap, Worker worker, Map<String, Component> clusterStateOfWorker, ServerListParam param) {
        item.setServerId(processId);
        final ServerChart.Usage usage = Optional.ofNullable(usageMap.get(processId)).orElse(ServerChart.Usage.create());
        item.setCpuUsage(usage.getCpuUsage());
        item.setMemoryUsage(usage.getMemoryUsage());
        item.setTs(usage.getTs());
        item.setServerId(processId);
        item.setServerName(Optional.ofNullable(worker).map(Worker::getHostname).orElse(""));
        item.setDeleted(Optional.ofNullable(worker).map(Worker::getDeleted).orElse(false));
        //set cluster state
        Component workerClusterStatus = clusterStateOfWorker.get(processId);
        item.setServerStatus(Optional.ofNullable(workerClusterStatus).map(Component::getStatus).orElse(null));
        item.setServerPingTime(Optional.ofNullable(worker).map(Worker::getWorkerStatus).map(ApiServerStatus::getActiveTime).orElse(null));
        item.setServerPingStatus(Optional.ofNullable(worker).map(Worker::getWorkerStatus).map(ApiServerStatus::getStatus).orElse(null));
        item.setQueryFrom(param.getStartAt());
        item.setQueryEnd(param.getEndAt());
        item.setGranularity(param.getGranularity().getType());
    }

    protected Worker findServerById(String serverId) {
        Criteria criteriaOfServer = Criteria.where(WORKER_TYPE).is(API_SERVER)
                .and("process_id").is(serverId);
        Query queryServer = Query.query(criteriaOfServer);
        queryServer.limit(1);
        Worker worker = workerRepository.findOne(queryServer).orElse(null);
        if (null == worker) {
            throw new BizException("server.not.exists");
        }
        return worker;
    }

    public ServerOverviewDetail serverOverviewDetail(ServerDetail param) {
        String serverId = param.getServerId();
        if (StringUtils.isBlank(serverId)) {
            throw new BizException(SERVER_ID_EMPTY);
        }
        ServerOverviewDetail result = new ServerOverviewDetail();
        Worker worker = findServerById(serverId);
        result.setServerName(Optional.ofNullable(worker.getHostname()).orElse(""));
        result.setServerId(serverId);
        Optional.ofNullable(worker.getWorkerStatus())
                .map(ApiServerStatus::getMetricValues)
                .ifPresent(u -> {
                    result.setCpuUsage(u.getCpuUsage());
                    Optional.ofNullable(u.getHeapMemoryUsageMax())
                            .ifPresent(max -> result.setMemoryUsage(ApiMetricsDelayInfoUtil.rate(u.getHeapMemoryUsage(), max)));
                    if (u.getLastUpdateTime() instanceof Number iNum) {
                        result.setUsagePingTime(iNum.longValue());
                    }
                });
        long delay = metricsRawMergeService.getDelay();
        TimeRangeUtil.rangeOf(result, param, delay, true);
        List<ApiMetricsRaw> apiMetricsRaws = metricsRawMergeService.merge(
                param,
                c -> c.and(PROCESS_ID).is(serverId).and("metricType").is(MetricTypes.SERER.getType()),
                Criteria.where("api_gateway_uuid").is(serverId),
                new String[]{"apiId", "processId", "timeGranularity", "timeStart", "reqCount", "errorCount", "delay", "workerInfoMap"}
                );
        result.setRequestCount(0L);
        long errorCount = metricsRawMergeService.errorCountGetter(apiMetricsRaws, e -> result.setRequestCount(result.getRequestCount() + e));
        result.setErrorCount(errorCount);
        result.setErrorRate(ApiMetricsDelayInfoUtil.rate(errorCount, result.getRequestCount()));
        metricsRawMergeService.baseDataCalculate(result, apiMetricsRaws, null);
        TopWorkerInServer topWorkerInServer = topWorkerInServer(apiMetricsRaws, param);
        result.setWorkerInfo(topWorkerInServer);
        return result;
    }

    public ServerChart serverChart(ServerChartParam param) {
        String serverId = param.getServerId();
        if (StringUtils.isBlank(serverId)) {
            throw new BizException(SERVER_ID_EMPTY);
        }
        ServerChart result = new ServerChart();
        long delay = metricsRawMergeService.getDelay();
        TimeRangeUtil.rangeOf(result, param, delay, false);
        //cpu&mem usage
        Criteria criteriaOfUsage = Criteria.where(PROCESS_ID).is(serverId)
                .and(PROCESS_TYPE).is(ServerUsage.ProcessType.API_SERVER.getType());
        List<? extends ServerUsage> allUsage = queryCpuUsageRecords(criteriaOfUsage, param.getRealStart(), param.getRealEnd(), param.getGranularity());
        ServerChart.Usage usage = this.mapUsage(allUsage, param.getRealStart(), param.getRealEnd(), param.getGranularity());
        result.setUsage(usage);
        //request chart & avg delay & p95 & p99
        List<ApiMetricsRaw> apiMetricsRaws = service.supplementMetricsRaw(
                param, false,
                c -> c.and(PROCESS_ID).is(serverId).and("metricType").is(MetricTypes.SERER.getType()),
                Criteria.where("api_gateway_uuid").is(serverId),
                new String[]{"apiId", "processId", "timeGranularity", "timeStart", "reqCount", "errorCount", "delay", "workerInfoMap"}
                );
        result.setRequest(ServerChart.Request.create());
        result.setDelay(ServerChart.Delay.create());
        result.setDBCost(ServerChart.DBCost.create());
        Map<Long, ServerChart.Item> collect = apiMetricsRaws.stream()
                .filter(Objects::nonNull)
                .filter(e -> Objects.nonNull(e.getTimeStart())).collect(
                        Collectors.groupingBy(
                                ApiMetricsRaw::getTimeStart,
                                Collectors.collectingAndThen(
                                        Collectors.toList(),
                                        rows -> {
                                            ServerChart.Item item = new ServerChart.Item();
                                            ApiMetricsRaw apiMetricsRaw = rows.get(0);
                                            long totalErrorCount = 0L;
                                            long reqCount = 0L;
                                            for (ApiMetricsRaw row : rows) {
                                                totalErrorCount += Optional.ofNullable(row.getErrorCount()).orElse(0L);
                                                reqCount += Optional.ofNullable(row.getReqCount()).orElse(0L);
                                            }
                                            List<Map<String, Number>> delays = ApiMetricsDelayInfoUtil.mergeItems(rows, ApiMetricsRaw::getDelay);
                                            List<Map<String, Number>> dbCosts = ApiMetricsDelayInfoUtil.mergeItems(rows, ApiMetricsRaw::getDbCost);
                                            item.setDelay(delays);
                                            item.setDbCost(dbCosts);
                                            item.setTs(apiMetricsRaw.getTimeStart());
                                            item.setRequestCount(reqCount);
                                            item.setErrorCount(totalErrorCount);
                                            return item;
                                        }
                                )
                        )
                );
        //fix time and sort by time
        //5秒点位最多攒5分钟
        //分钟点位最多攒1小时
        //其他不赞批，直接用
        int maxDepth = switch (param.getGranularity()) {
            case SECOND_FIVE, MINUTE -> 60;
            default -> 1;
        };
        List<List<Map<String, Number>>> delays = new ArrayList<>();
        List<List<Map<String, Number>>> dbCosts = new ArrayList<>();
        AtomicInteger size = new AtomicInteger(0);
        List<ServerChart.Item> items = ChartSortUtil.fixAndSort(collect,
                param.getWindowsStart(), param.getEndAt(), param.getGranularity(),
                ServerChart.Item::create,
                item -> {
                    delays.add(item.getDelay());
                    dbCosts.add(item.getDbCost());
                    size.addAndGet(1);
                    if (size.get() > maxDepth) {
                        delays.remove(0);
                        dbCosts.remove(0);
                        size.addAndGet(-1);
                    }
                    if (size.get() >= maxDepth) {
                        item.setDelays(new ArrayList<>(new ArrayList<>(delays)));
                        item.setDbCosts(new ArrayList<>(new ArrayList<>(dbCosts)));
                    }
                }
        );
        items.parallelStream()
                .forEach(this::handler);
        for (ServerChart.Item item : items) {
            if (item.getTs() < param.getFixStart() || item.getTs() >= param.getEndAt()) {
                continue;
            }
            result.add(item);
        }
        return result;
    }

    void handler(ServerChart.Item item) {
        List<Map<String, Number>> delay = item.getDelay();
        Long errorCountNum = item.getErrorCount();
        Long requestCount = item.getRequestCount();
        if (requestCount > 0L) {
            ApiMetricsDelayUtil.readMaxAndMin(delay, item::setMaxDelay, item::setMinDelay);
            Double errorRate = ApiMetricsDelayInfoUtil.rate(errorCountNum, requestCount);
            item.setRequestCount(requestCount);
            item.setErrorRate(errorRate);
            item.setErrorCount(errorCountNum);

            ApiMetricsDelayUtil.Sum sumOfDbCost = ApiMetricsDelayUtil.sum(item.getDbCost());
            long dbCostTotal = sumOfDbCost.getTotal();
            ApiMetricsDelayUtil.readMaxAndMin(item.getDbCost(), item::setDbCostMax, item::setDbCostMin);
            ApiMetricsDelayUtil.Sum sumOfDelay = ApiMetricsDelayUtil.sum(delay);
            item.setAvg(1.0D * sumOfDelay.getTotal() / requestCount);
            item.setDbCostAvg(1.0D * dbCostTotal / requestCount);
        }
//        if (item.isEmpty() || requestCount <= 0L) {
//            return;
//        }
        AtomicReference<ApiMetricsDelayUtil.Sum> sumOf = new AtomicReference<>(null);
        Optional.ofNullable(item.getDelays()).ifPresent(delays -> {
            List<Map<String, Number>> mergedDelay = ApiMetricsDelayUtil.merge(delays);
            sumOf.set(ApiMetricsDelayUtil.sum(mergedDelay));
            long totalCount = sumOf.get().getCount();
            item.setP95(ApiMetricsDelayUtil.p95(mergedDelay, totalCount));
            item.setP99(ApiMetricsDelayUtil.p99(mergedDelay, totalCount));
        });
        Optional.ofNullable(item.getDbCosts()).ifPresent(dbCosts -> {
            List<Map<String, Number>> mergedDBCost = ApiMetricsDelayUtil.merge(dbCosts);
            long totalCount = 0L;
            if (null == sumOf.get()) {
                totalCount = ApiMetricsDelayUtil.sum(mergedDBCost).getCount();
            } else {
                totalCount = sumOf.get().getCount();
            }
            item.setDbCostP95(ApiMetricsDelayUtil.p95(mergedDBCost, totalCount));
            item.setDbCostP99(ApiMetricsDelayUtil.p99(mergedDBCost, totalCount));
        });
    }

    public TopWorkerInServer topWorkerInServer(TopWorkerInServerParam param) {
        String serverId = param.getServerId();
        if (StringUtils.isBlank(serverId)) {
            throw new BizException(SERVER_ID_EMPTY);
        }
        TopWorkerInServer result = new TopWorkerInServer();
        Worker worker = findServerById(serverId);
        Collection<ApiServerWorkerInfo> workers = Optional.ofNullable(worker.getWorkerStatus())
                .map(ApiServerStatus::getWorkers)
                .map(Map::values)
                .orElse(new ArrayList<>());
        Optional<Double> maxCpu = workers.stream().map(ApiServerWorkerInfo::getMetricValues)
                .filter(Objects::nonNull)
                .map(MetricInfo::getCpuUsage)
                .filter(Objects::nonNull)
                .max(Double::compareTo);
        Optional<Double> minCpu = workers.stream().map(ApiServerWorkerInfo::getMetricValues)
                .filter(Objects::nonNull)
                .map(MetricInfo::getCpuUsage)
                .filter(Objects::nonNull)
                .min(Double::compareTo);
        maxCpu.ifPresent(result::setCpuUsageMax);
        minCpu.ifPresent(result::setCpuUsageMin);
        TopWorkerInServerParam.TAG tag = TopWorkerInServerParam.TAG.fromValue(param.getTag());
        if (tag != TopWorkerInServerParam.TAG.ALL) {
            return result;
        }
        result.setWorkerList(new ArrayList<>());
        long delay = metricsRawMergeService.getDelay();
        TimeRangeUtil.rangeOf(result, param, delay, true);
        Set<String> workerOid = new HashSet<>(Optional.ofNullable(worker.getWorkerStatus())
                .map(ApiServerStatus::getWorkers)
                .map(Map::keySet)
                .orElse(new HashSet<>()));
        List<String> workerIds = workers.stream().
                filter(Objects::nonNull)
                .map(ApiServerWorkerInfo::getOid)
                .filter(StringUtils::isNotBlank)
                .toList();
        workerOid.addAll(workerIds);
        if (workerOid.isEmpty()) {
            return result;
        }
        List<WorkerCallEntity> callOfWorker = service.supplementWorkerCall(param);
        Map<String, TopWorkerInServer.TopWorkerInServerItem> workerInfoMap = callOfWorker.stream()
                .filter(Objects::nonNull)
                .filter(e -> StringUtils.isNotBlank(e.getWorkOid()))
                .collect(Collectors.groupingBy(
                        WorkerCallEntity::getWorkOid,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                infos -> {
                                    TopWorkerInServer.TopWorkerInServerItem item = new TopWorkerInServer.TopWorkerInServerItem();
                                    long errorCount = 0L;
                                    long reqCount = 0L;
                                    for (WorkerCallEntity info : infos) {
                                        errorCount += Optional.ofNullable(info.getErrorCount()).orElse(0L);
                                        reqCount += Optional.ofNullable(info.getReqCount()).orElse(0L);
                                    }
                                    item.setRequestCount(reqCount);
                                    item.setErrorRate(ApiMetricsDelayInfoUtil.rate(errorCount, reqCount));
                                    return item;
                                }
                        )
                ));
        Criteria criteriaOfUsage = Criteria.where(PROCESS_ID).is(serverId)
                .and(PROCESS_TYPE).is(ServerUsage.ProcessType.API_SERVER_WORKER.getType());
        List<? extends UsageBase> allUsage = queryCpuUsageRecords(criteriaOfUsage, param.getRealStart(), param.getRealEnd(), param.getGranularity());
        Map<String, ServerChart.Usage> usageMap = allUsage.stream()
                .filter(Objects::nonNull)
                .filter(e -> StringUtils.isNotBlank(e.getWorkOid()))
                .collect(Collectors.groupingBy(UsageBase::getWorkOid, Collectors.collectingAndThen(
                        Collectors.toList(),
                        items -> this.mapUsage(items, param.getRealStart(), param.getRealEnd(), param.getGranularity())
                )));
        for (ApiServerWorkerInfo e : workers) {
            if (null == e || StringUtils.isBlank(e.getOid())) {
                continue;
            }
            TopWorkerInServer.TopWorkerInServerItem item = Optional.ofNullable(workerInfoMap.get(e.getOid())).orElse(new TopWorkerInServer.TopWorkerInServerItem());
            item.setWorkerId(e.getOid());
            item.setWorkerName(e.getName());
            item.setUsage(Optional.ofNullable(usageMap.get(e.getOid())).orElse(new ServerChart.Usage()));
            result.getWorkerList().add(item);
        }
        result.setQueryFrom(param.getStartAt());
        result.setQueryEnd(param.getEndAt());
        result.setGranularity(param.getGranularity().getType());
        result.getWorkerList().sort((a, b) -> {
            int n1 = extractIndex(a.getWorkerName());
            int n2 = extractIndex(b.getWorkerName());
            return Integer.compare(n1, n2);
        });
        return result;
    }

    public TopWorkerInServer topWorkerInServer(List<ApiMetricsRaw> apiMetricsRaws, ServerDetail param) {
        String serverId = param.getServerId();
        if (StringUtils.isBlank(serverId)) {
            throw new BizException(SERVER_ID_EMPTY);
        }
        TopWorkerInServer result = new TopWorkerInServer();
        Worker worker = findServerById(serverId);
        Collection<ApiServerWorkerInfo> workers = Optional.ofNullable(worker.getWorkerStatus())
                .map(ApiServerStatus::getWorkers)
                .map(Map::values)
                .orElse(new ArrayList<>());
        Optional<Double> maxCpu = workers.stream().map(ApiServerWorkerInfo::getMetricValues)
                .filter(Objects::nonNull)
                .map(MetricInfo::getCpuUsage)
                .filter(Objects::nonNull)
                .max(Double::compareTo);
        Optional<Double> minCpu = workers.stream().map(ApiServerWorkerInfo::getMetricValues)
                .filter(Objects::nonNull)
                .map(MetricInfo::getCpuUsage)
                .filter(Objects::nonNull)
                .min(Double::compareTo);
        maxCpu.ifPresent(result::setCpuUsageMax);
        minCpu.ifPresent(result::setCpuUsageMin);
        result.setWorkerList(new ArrayList<>());
        Set<String> workerOid = new HashSet<>(Optional.ofNullable(worker.getWorkerStatus())
                .map(ApiServerStatus::getWorkers)
                .map(Map::keySet)
                .orElse(new HashSet<>()));
        List<String> workerIds = workers.stream().
                filter(Objects::nonNull)
                .map(ApiServerWorkerInfo::getOid)
                .filter(StringUtils::isNotBlank)
                .toList();
        workerOid.addAll(workerIds);
        if (workerOid.isEmpty()) {
            return result;
        }

        Map<String, TopWorkerInServer.TopWorkerInServerItem> workerInfoMap = new HashMap<>();
        apiMetricsRaws.stream()
                .map(ApiMetricsRaw::getWorkerInfoMap)
                .filter(Objects::nonNull)
                .forEach(list -> list.stream()
                        .filter(Objects::nonNull)
                        .filter(e -> StringUtils.isNotBlank(e.getWorkerOid()))
                        .forEach(w -> {
                            TopWorkerInServer.TopWorkerInServerItem info = workerInfoMap.computeIfAbsent(w.getWorkerOid(), k -> new TopWorkerInServer.TopWorkerInServerItem());
                            info.setRequestCount(ApiMetricsDelayInfoUtil.sum(info.getRequestCount(), w.getReqCount()));
                            long errorCount = ApiMetricsDelayInfoUtil.sum(info.getErrorCount(), w.getErrorCount());
                            info.setErrorCount(errorCount);
                            if (errorCount > 0L) {
                                info.setErrorRate(ApiMetricsDelayInfoUtil.rate(errorCount, info.getRequestCount()));
                            }
                        }));
        Criteria criteriaOfUsage = Criteria.where(PROCESS_ID).is(serverId)
                .and(PROCESS_TYPE).is(ServerUsage.ProcessType.API_SERVER_WORKER.getType());
        List<? extends UsageBase> allUsage = queryCpuUsageRecords(criteriaOfUsage, param.getRealStart(), param.getRealEnd(), param.getGranularity());
        Map<String, ServerChart.Usage> usageMap = allUsage.stream()
                .filter(Objects::nonNull)
                .filter(e -> StringUtils.isNotBlank(e.getWorkOid()))
                .collect(Collectors.groupingBy(UsageBase::getWorkOid, Collectors.collectingAndThen(
                        Collectors.toList(),
                        items -> this.mapUsage(items, param.getRealStart(), param.getRealEnd(), param.getGranularity())
                )));
        for (ApiServerWorkerInfo e : workers) {
            if (null == e || StringUtils.isBlank(e.getOid())) {
                continue;
            }
            TopWorkerInServer.TopWorkerInServerItem item = Optional.ofNullable(workerInfoMap.get(e.getOid())).orElse(new TopWorkerInServer.TopWorkerInServerItem());
            item.setWorkerId(e.getOid());
            item.setWorkerName(e.getName());
            item.setUsage(Optional.ofNullable(usageMap.get(e.getOid())).orElse(new ServerChart.Usage()));
            result.getWorkerList().add(item);
        }
        result.setQueryFrom(param.getStartAt());
        result.setQueryEnd(param.getEndAt());
        result.setGranularity(param.getGranularity().getType());
        result.getWorkerList().sort((a, b) -> {
            int n1 = extractIndex(a.getWorkerName());
            int n2 = extractIndex(b.getWorkerName());
            return Integer.compare(n1, n2);
        });
        return result;
    }

    protected int extractIndex(String name) {
        if (StringUtils.isBlank(name)) {
            return Integer.MAX_VALUE;
        }
        int indexOf = name.lastIndexOf('-');
        if (indexOf <= 0) {
            return Integer.MAX_VALUE;
        }
        String substring = name.substring(name.lastIndexOf('-') + 1);
        if (StringUtils.isBlank(substring)) {
            return Integer.MAX_VALUE;
        }
        try {
            return Integer.parseInt(substring);
        } catch (NumberFormatException e) {
            return Integer.MAX_VALUE;
        }
    }

    public ApiDetail apiOverviewDetail(ApiDetailParam param) {
        ApiDetail result = new ApiDetail();
        long delay = metricsRawMergeService.getDelay();
        TimeRangeUtil.rangeOf(result, param, delay, true);
        List<ApiMetricsRaw> apiMetricsRaws = findRowByApiId(
                param.getApiId(),
                param,
                MetricTypes.API,
                new String[]{"apiId", "processId", "timeGranularity", "timeStart", "reqCount", "errorCount", "delay"});
        ObjectId apiId = MongoUtils.toObjectId(param.getApiId());
        result.setApiName(param.getApiId());
        result.setApiPath(param.getApiId());
        if (null != apiId) {
            Criteria criteriaOfApi = Criteria.where("_id").is(apiId);
            Query queryApiInfo = Query.query(criteriaOfApi);
            queryApiInfo.fields().include("name", "apiVersion", "basePath", "prefix");
            queryApiInfo.limit(1);
            ModulesDto allApi = modulesService.findOne(queryApiInfo);
            Optional.ofNullable(allApi).ifPresent(api -> {
                result.setApiName(api.getName());
                String path = ApiPathUtil.apiPath(api.getApiVersion(), api.getBasePath(), api.getPrefix());
                result.setApiPath(path);
            });
        }
        if (!CollectionUtils.isEmpty(apiMetricsRaws)) {
            long totalRequestCount = apiMetricsRaws.stream().mapToLong(ApiMetricsRaw::getReqCount).sum();
            long totalErrorCount = apiMetricsRaws.stream().mapToLong(ApiMetricsRaw::getErrorCount).sum();
            result.setRequestCount(totalRequestCount);
            result.setErrorRate(ApiMetricsDelayInfoUtil.rate(totalErrorCount, totalRequestCount));
            result.setErrorCount(totalErrorCount);
            metricsRawMergeService.baseDataCalculate(result, apiMetricsRaws, null);
        }
        return result;
    }

    protected List<ApiMetricsRaw> findRowByApiId(String apiId, QueryBase param, MetricTypes metricTypes, String[] filterFields) {
        if (StringUtils.isBlank(apiId)) {
            throw new BizException("api.id.empty");
        }
        return metricsRawMergeService.merge(
                param,
                c -> c.and("apiId").is(apiId).and("metricType").is(metricTypes.getType()),
                Criteria.where("allPathId").is(apiId),
                filterFields
        );
    }

    public List<ApiOfEachServer> apiOfEachServer(ApiWithServerDetail param) {
        long delay = metricsRawMergeService.getDelay();
        TimeRangeUtil.rangeOf(param, delay, true);
        List<ApiMetricsRaw> apiMetricsRaws = findRowByApiId(
                param.getApiId(),
                param,
                MetricTypes.API_SERVER,
                new String[]{"apiId", "processId", "timeGranularity", "timeStart", "reqCount", "errorCount", "delay"}
        );
        if (apiMetricsRaws.isEmpty()) {
            return ApiOfEachServer.supplement(new ArrayList<>(), activeWorkers(null));
        }
        List<String> serverIds = apiMetricsRaws.stream()
                .map(ApiMetricsRaw::getProcessId)
                .filter(StringUtils::isNotBlank)
                .distinct()
                .toList();
        if (serverIds.isEmpty()) {
            return ApiOfEachServer.supplement(new ArrayList<>(), activeWorkers(null));
        }
        Criteria criteriaOfServer = Criteria.where(WORKER_TYPE).is(API_SERVER)
                .and("process_id").in(serverIds);
        Query queryOfServer = Query.query(criteriaOfServer);
        List<Worker> serverList = workerRepository.findAll(queryOfServer);
        Map<String, ApiOfEachServer> serverMap = serverList.stream().collect(Collectors.toMap(Worker::getProcessId, e -> {
            ApiOfEachServer item = new ApiOfEachServer();
            item.setServerId(e.getProcessId());
            item.setServerName(e.getHostname());
            return item;
        }, (e1, e2) -> e2));
        Map<String, ApiOfEachServer> collect = apiMetricsRaws.stream()
                .filter(Objects::nonNull)
                .filter(e -> StringUtils.isNotBlank(e.getProcessId()))
                .collect(
                        Collectors.groupingBy(
                                ApiMetricsRaw::getProcessId,
                                Collectors.collectingAndThen(
                                        Collectors.toList(),
                                        rows -> {
                                            ApiMetricsRaw first = rows.get(0);
                                            String processId = first.getProcessId();
                                            ApiOfEachServer item = Optional.ofNullable(serverMap.get(processId)).orElse(new ApiOfEachServer());
                                            item.setRequestCount(rows.stream().mapToLong(ApiMetricsRaw::getReqCount).sum());
                                            long errorCount = rows.stream().mapToLong(ApiMetricsRaw::getErrorCount).sum();
                                            item.setErrorRate(ApiMetricsDelayInfoUtil.rate(errorCount, item.getRequestCount()));
                                            item.setErrorCount(errorCount);
                                            metricsRawMergeService.baseDataCalculate(item, rows, null);
                                            item.setQueryFrom(param.getStartAt());
                                            item.setQueryEnd(param.getEndAt());
                                            item.setGranularity(param.getGranularity().getType());
                                            return item;
                                        }
                                ))
                );
        List<ApiOfEachServer> apiOfEachServers = new ArrayList<>(collect.values());
        List<String> existsServerIds = serverList.stream()
                .filter(Objects::nonNull)
                .map(Worker::getProcessId)
                .filter(StringUtils::isNotBlank)
                .toList();
        ApiOfEachServer.supplement(apiOfEachServers, activeWorkers(existsServerIds));
        ChartSortUtil.sort(apiOfEachServers, param.getSortInfo(), ApiOfEachServer.class);
        return apiOfEachServers;
    }

    public ChartAndDelayOfApi delayOfApi(ApiChart param) {
        ChartAndDelayOfApi result = ChartAndDelayOfApi.create();
        long delay = metricsRawMergeService.getDelay();
        TimeRangeUtil.rangeOf(result, param, delay, false);
        if (StringUtils.isBlank(param.getApiId())) {
            throw new BizException("api.id.empty");
        }
        List<ApiMetricsRaw> apiMetricsRaws = service.supplementMetricsRaw(
                param, false,
                c -> c.and("apiId").is(param.getApiId()).and("metricType").is(MetricTypes.API.getType()),
                Criteria.where("allPathId").is(param.getApiId()),
                new String[]{"apiId", "processId", "timeGranularity", "timeStart", "reqCount", "errorCount", "bytes", "delay", "dbCost"}
        );
        if (apiMetricsRaws.isEmpty()) {
            return result;
        }
        Map<Long, ChartAndDelayOfApi.Item> collect = apiMetricsRaws.stream()
                .filter(Objects::nonNull)
                .filter(e -> Objects.nonNull(e.getTimeStart()))
                .collect(
                        Collectors.groupingBy(
                                ApiMetricsRaw::getTimeStart,
                                Collectors.collectingAndThen(
                                        Collectors.toList(),
                                        rows -> {
                                            ApiMetricsRaw apiMetricsRaw = rows.get(0);
                                            long timeStart = apiMetricsRaw.getTimeStart();
                                            ChartAndDelayOfApi.Item item = new ChartAndDelayOfApi.Item();
                                            long totalBytes = rows.stream().map(ApiMetricsRaw::getBytes)
                                                    .map(ApiMetricsDelayUtil::sum)
                                                    .mapToLong(ApiMetricsDelayUtil.Sum::getTotal)
                                                    .sum();
                                            List<Map<String, Number>> mergedDelay = ApiMetricsDelayInfoUtil.mergeItems(rows, ApiMetricsRaw::getDelay);
                                            item.setDelay(mergedDelay);
                                            item.setTs(timeStart);
                                            item.setTotalBytes(totalBytes);
                                            List<Map<String, Number>> mergedDBCost = ApiMetricsDelayInfoUtil.mergeItems(rows, ApiMetricsRaw::getDbCost);
                                            item.setDbCost(mergedDBCost);
                                            return item;
                                        }
                                )
                        )
                );
        //fix time and sort by time
        //5秒点位最多攒5分钟
        //分钟点位最多攒1小时
        //其他不赞批，直接用
        int maxDepth = switch (param.getGranularity()) {
            case SECOND_FIVE, MINUTE -> 60;
            default -> 1;
        };
        List<List<Map<String, Number>>> delays = new ArrayList<>();
        List<Long> bytes = new ArrayList<>();
        List<List<Map<String, Number>>> dbCosts = new ArrayList<>();
        List<ChartAndDelayOfApi.Item> items = ChartSortUtil.fixAndSort(collect,
                param.getWindowsStart(), param.getEndAt(), param.getGranularity(),
                ChartAndDelayOfApi.Item::create,
                item -> {
                    delays.add(item.getDelay());
                    bytes.add(item.getTotalBytes());
                    dbCosts.add(item.getDbCost());
                    int size = delays.size();
                    if (size > maxDepth) {
                        delays.remove(0);
                        bytes.remove(0);
                        dbCosts.remove(0);
                    }
                    if (delays.size() >= maxDepth) {
                        item.point(delays, bytes, dbCosts);
                    }
                }
        );
        items.parallelStream().forEach(this::mapping);
        for (ChartAndDelayOfApi.Item item : items) {
            if (item.getTs() < param.getFixStart() || item.getTs() >= param.getEndAt()) {
                continue;
            }
            result.add(item);
        }
        return result;
    }

    public void mapping(ChartAndDelayOfApi.Item item) {
        ApiMetricsDelayUtil.Sum sumOfDelay = ApiMetricsDelayUtil.sum(item.getDelay());
        long totalDelayMs = sumOfDelay.getTotal();
        long reqCount = sumOfDelay.getCount();
        long totalDbCost = ApiMetricsDelayUtil.sum(item.getDbCost()).getTotal();
        ApiMetricsDelayUtil.readMaxAndMin(item.getDbCost(), item::setDbCostMax, item::setDbCostMin);
        if (reqCount > 0L) {
            item.setRequestCostAvg(1.0D * totalDelayMs / reqCount);
            item.setDbCostAvg(1.0D * totalDbCost / reqCount);
        } else {
            item.setRequestCostAvg(0D);
            item.setDbCostAvg(0D);
        }
        ApiMetricsDelayUtil.readMaxAndMin(item.getDelay(), item::setMaxDelay, item::setMinDelay);
        long totalBytes = Optional.ofNullable(item.getTotalBytes()).orElse(0L);
        item.setRps(totalDelayMs > 0L ? (totalBytes * 1000.0D / totalDelayMs) : 0D);
//        if (item.isEmpty() || reqCount <= 0L) {
//            return;
//        }
        Long totalCount = null;
        if (null != item.getDelays()) {
            List<Map<String, Number>> mergedDelays = ApiMetricsDelayUtil.merge(item.getDelays());
            totalCount = ApiMetricsDelayUtil.sum(mergedDelays).getCount();
            item.setP95(ApiMetricsDelayUtil.p95(mergedDelays, totalCount));
            item.setP99(ApiMetricsDelayUtil.p99(mergedDelays, totalCount));
        }
        if (null != item.getDbCosts()) {
            List<Map<String, Number>> mergeDdCosts = ApiMetricsDelayUtil.merge(item.getDbCosts());
            if (null == totalCount) {
                totalCount = ApiMetricsDelayUtil.sum(mergeDdCosts).getCount();
            }
            item.setDbCostP95(ApiMetricsDelayUtil.p95(mergeDdCosts, totalCount));
            item.setDbCostP99(ApiMetricsDelayUtil.p99(mergeDdCosts, totalCount));
        }
    }

    protected Map<String, Worker> activeWorkers(Collection<String> ignoreIds) {
        Criteria criteriaOfServer = Criteria.where(WORKER_TYPE).is(API_SERVER)
                .and("deleted").ne(true)
                .and("isDeleted").ne(true);
        if (!CollectionUtils.isEmpty(ignoreIds)) {
            criteriaOfServer.and("processId").nin(ignoreIds);
        }
        Query queryOfServer = Query.query(criteriaOfServer);
        List<Worker> serverList = workerRepository.findAll(queryOfServer);
        return serverList.stream()
                .filter(Objects::nonNull)
                .filter(e -> Objects.nonNull(e.getProcessId()))
                .collect(Collectors.toMap(Worker::getProcessId, e -> e, (e1, e2) -> e1));
    }
}
