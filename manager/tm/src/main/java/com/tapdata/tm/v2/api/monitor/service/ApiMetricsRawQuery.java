package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.cluster.dto.Component;
import com.tapdata.tm.cluster.entity.ClusterStateEntity;
import com.tapdata.tm.cluster.repository.ClusterStateRepository;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.modules.constant.ModuleStatusEnum;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.utils.ApiMetricsDelayUtil;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.v2.api.monitor.main.dto.ApiDetail;
import com.tapdata.tm.v2.api.monitor.main.dto.ApiItem;
import com.tapdata.tm.v2.api.monitor.main.dto.ApiOfEachServer;
import com.tapdata.tm.v2.api.monitor.main.dto.ApiTopOnHomepage;
import com.tapdata.tm.v2.api.monitor.main.dto.ChartAndDelayOfApi;
import com.tapdata.tm.v2.api.monitor.main.dto.DataValueBase;
import com.tapdata.tm.v2.api.monitor.main.dto.ServerChart;
import com.tapdata.tm.v2.api.monitor.main.dto.ServerItem;
import com.tapdata.tm.v2.api.monitor.main.dto.ServerOverviewDetail;
import com.tapdata.tm.v2.api.monitor.main.dto.ServerTopOnHomepage;
import com.tapdata.tm.v2.api.monitor.main.dto.TopApiInServer;
import com.tapdata.tm.v2.api.monitor.main.dto.TopWorkerInServer;
import com.tapdata.tm.v2.api.monitor.main.dto.ValueBase;
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
import com.tapdata.tm.v2.api.monitor.utils.ApiMetricsDelayInfoUtil;
import com.tapdata.tm.v2.api.monitor.utils.ApiPathUtil;
import com.tapdata.tm.v2.api.monitor.utils.ChartSortUtil;
import com.tapdata.tm.v2.api.usage.repository.ServerUsageMetricRepository;
import com.tapdata.tm.v2.api.usage.repository.UsageRepository;
import com.tapdata.tm.worker.dto.ApiServerStatus;
import com.tapdata.tm.worker.dto.ApiServerWorkerInfo;
import com.tapdata.tm.worker.dto.MetricInfo;
import com.tapdata.tm.worker.entity.ServerUsage;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongConsumer;
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
public class ApiMetricsRawQuery {
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

    public ApiMetricsRawQuery() {

    }

    public ServerTopOnHomepage serverTopOnHomepage(QueryBase param) {
        final ServerTopOnHomepage result = ServerTopOnHomepage.create();
        final Criteria criteria = ParticleSizeAnalyzer.of(result, param);
        final Query query = Query.query(criteria);
        final List<ApiMetricsRaw> apiMetricsRaws = service.supplementMetricsRaw(service.find(query), param);
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
            final long errorServer = errorCount(apiMetricsRaws, ApiMetricsRaw::getProcessId);
            final long errorApi = errorCount(apiMetricsRaws, ApiMetricsRaw::getApiId);
            result.setErrorCount(errorCount.get());
            result.setTotalErrorRate(ApiMetricsDelayInfoUtil.rate(errorCount.get(), totalRequestCount));
            result.setNotHealthyApiCount(errorApi);
            result.setNotHealthyServerCount(errorServer);
            baseDataCalculate(result, apiMetricsRaws, result::setResponseTime);
        }
        return result;
    }

    protected long errorCount(List<ApiMetricsRaw> apiMetricsRaws, Function<ApiMetricsRaw, String> groupBy) {
        return apiMetricsRaws.stream()
                .filter(Objects::nonNull)
                .filter(e -> Objects.nonNull(groupBy.apply(e)))
                .collect(Collectors.groupingBy(
                        groupBy,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                e -> {
                                    final ApiMetricsRaw errorOne = e.stream()
                                            .filter(i -> i.getErrorCount() > 0)
                                            .findFirst()
                                            .orElse(null);
                                    return null == errorOne ? 0 : 1;
                                }
                        )
                )).values().stream().filter(e -> e > 0).count();
    }

    public List<ServerItem> serverOverviewList(ServerListParam param) {
        List<ServerItem> result = new ArrayList<>();
        Criteria criteria = ParticleSizeAnalyzer.of(param);
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
        if (StringUtils.isNotBlank(serverMatchName)) {
            criteria.and(PROCESS_ID).in(serverMap.keySet());
        }

        final Query query = Query.query(criteria);
        List<ApiMetricsRaw> raws = service.find(query);
        final List<ApiMetricsRaw> apiMetricsRaws = service.supplementMetricsRaw(raws, param);

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
        List<? extends ServerUsage> allUsage = queryCpuUsageRecords(criteriaOfUsage, param.getStartAt(), param.getEndAt(), param.getGranularity());
        Map<String, ServerChart.Usage> usageMap = allUsage.stream()
                .filter(Objects::nonNull)
                .filter(e -> StringUtils.isNotBlank(e.getProcessId()))
                .collect(Collectors.groupingBy(ServerUsage::getProcessId, Collectors.collectingAndThen(
                        Collectors.toList(),
                        items -> this.mapUsage(items, param.getStartAt(), param.getEndAt(), param.getGranularity())
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
                                    int total = item.getRequestCount().intValue();
                                    long errorCount = errorCountGetter(infos, e -> item.setRequestCount(item.getRequestCount() + e));
                                    if (total > 0) {
                                        item.setErrorRate(ApiMetricsDelayInfoUtil.rate(errorCount, item.getRequestCount()));
                                        item.setErrorCount(errorCount);
                                        baseDataCalculate(item, apiMetricsRaws, null);
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

    protected <T extends ServerUsage> List<T> queryCpuUsageRecords(Criteria criteriaBase, long queryStart, long queryEnd, int type) {
        criteriaBase.andOperator(
                Criteria.where("lastUpdateTime").gte(queryStart * 1000L),
                Criteria.where("lastUpdateTime").lt(queryEnd * 1000L)
        );
        if (type == 0) {
            criteriaBase.and("type").in(List.of(0, 1, 2));
            Query queryOfUsage = Query.query(criteriaBase);
            return (List<T>) usageRepository.findAll(queryOfUsage);
        }
        Query query = Query.query(criteriaBase);
        return (List<T>) serverUsageMetricRepository.findAll(query);
    }

    protected ServerChart.Usage mapUsage(List<? extends ServerUsage> infos, long startAt, long endAt, int granularity) {
        final ServerChart.Usage usage = ServerChart.Usage.create();
        // Calculate step based on granularity
        long step = ApiMetricsDelayInfoUtil.stepByGranularity(granularity);
        if (infos.isEmpty()) {
            return usage;
        }
        // Sort by time
        infos.sort(Comparator.comparingLong(ServerUsage::getLastUpdateTime));
        // Fill gaps before first data point
        long currentTime = startAt;
        long firstDataTime = infos.get(0).getLastUpdateTime() / 1000L;
        while (currentTime < firstDataTime) {
            usage.addEmpty(currentTime, granularity != 0);
            currentTime += step;
        }
        // Process each data point
        long lastProcessedTime = currentTime;
        for (ServerUsage info : infos) {
            long ts = info.getLastUpdateTime() / 1000L;
            // Fill any gaps between data points
            while (lastProcessedTime < ts) {
                usage.addEmpty(lastProcessedTime, granularity != 0);
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
            usage.addEmpty(fillTime, granularity != 0);
            fillTime += step;
        }
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
        item.setGranularity(param.getGranularity());
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
                    Optional.ofNullable(u.getHeapMemoryUsageMax()).ifPresent(max -> result.setMemoryUsage(ApiMetricsDelayInfoUtil.rate(u.getHeapMemoryUsage(), max)));
                    if (u.getLastUpdateTime() instanceof Number iNum) {
                        result.setUsagePingTime(iNum.longValue());
                    }
                });
        Criteria criteria = ParticleSizeAnalyzer.of(result, param);
        criteria.and("processId").is(serverId);
        final Query query = Query.query(criteria);
        final List<ApiMetricsRaw> apiMetricsRaws = service.supplementMetricsRaw(service.find(query), param, c -> c.and("processId").is(serverId), Criteria.where("api_gateway_uuid").is(serverId));
        result.setRequestCount(0L);
        long errorCount = errorCountGetter(apiMetricsRaws, e -> result.setRequestCount(result.getRequestCount() + e));
        result.setErrorRate(ApiMetricsDelayInfoUtil.rate(errorCount, result.getRequestCount()));
        baseDataCalculate(result, apiMetricsRaws, null);
        return result;
    }

    public ServerChart serverChart(ServerChartParam param) {
        //@todo
        String serverId = param.getServerId();
        if (StringUtils.isBlank(serverId)) {
            throw new BizException(SERVER_ID_EMPTY);
        }
        ServerChart result = new ServerChart();
        Criteria criteria = ParticleSizeAnalyzer.of(result, param);
        criteria.and(PROCESS_ID).is(serverId);

        //cpu&mem usage
        Criteria criteriaOfUsage = Criteria.where(PROCESS_ID).is(serverId)
                .and(PROCESS_TYPE).is(ServerUsage.ProcessType.API_SERVER.getType());
        List<? extends ServerUsage> allUsage = queryCpuUsageRecords(criteriaOfUsage, param.getStartAt(), param.getEndAt(), param.getGranularity());
        ServerChart.Usage usage = this.mapUsage(allUsage, param.getStartAt(), param.getEndAt(), param.getGranularity());
        result.setUsage(usage);

        //request chart & avg delay & p95 & p99
        Query query = Query.query(criteria);
        List<ApiMetricsRaw> apiMetricsRaws = service.supplementMetricsRaw(service.find(query), param, c -> criteria.and(PROCESS_ID).is(serverId), Criteria.where("api_gateway_uuid").is(serverId));
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
                                            long totalErrorCount = rows.stream().mapToLong(ApiMetricsRaw::getErrorCount).sum();
                                            long reqCount = rows.stream().mapToLong(ApiMetricsRaw::getReqCount).sum();
                                            List<Map<Long, Integer>> delays = ApiMetricsDelayInfoUtil.mergeItems(rows, ApiMetricsRaw::getDelay);
                                            List<Map<Long, Integer>> dbCosts = ApiMetricsDelayInfoUtil.mergeItems(rows, ApiMetricsRaw::getDbCost);
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
            case 0, 1 -> 60;
            default -> 1;
        };
        List<List<Map<Long, Integer>>> delays = new ArrayList<>();
        List<List<Map<Long, Integer>>> dbCosts = new ArrayList<>();
        List<Long> errorCount = new ArrayList<>();
        List<ServerChart.Item> items = ChartSortUtil.fixAndSort(collect,
                param.getQStart(), param.getEndAt(), param.getGranularity(),
                ServerChart.Item::create,
                item -> {
                    List<Map<Long, Integer>> delay = item.getDelay();
                    delays.add(delay);
                    errorCount.add(item.getErrorCount());
                    dbCosts.add(item.getDbCost());
                    if (delays.size() > maxDepth) {
                        delays.remove(0);
                        errorCount.remove(0);
                        dbCosts.remove(0);
                    }

                    List<Map<Long, Integer>> mergedDelay = ApiMetricsDelayUtil.merge(delays);
                    Long reqTotalDelay = ApiMetricsDelayUtil.sum(mergedDelay);
                    Long reqCount = ApiMetricsDelayUtil.sum(mergedDelay, (iKey, iCount) -> iCount.longValue());
                    ApiMetricsDelayUtil.readMaxAndMin(mergedDelay, item::setMaxDelay, item::setMinDelay);

                    long totalErrorCount = errorCount.stream().filter(Objects::nonNull).mapToLong(Long::longValue).sum();
                    Double errorRate = ApiMetricsDelayInfoUtil.rate(totalErrorCount, reqCount);
                    item.setRequestCount(reqCount);
                    item.setErrorRate(errorRate);
                    item.setErrorCount(totalErrorCount);


                    List<Map<Long, Integer>> mergedDBCost = ApiMetricsDelayUtil.merge(dbCosts);
                    Long dbCostTotal = ApiMetricsDelayUtil.sum(dbCosts);
                    ApiMetricsDelayUtil.readMaxAndMin(mergedDBCost, item::setDbCostMax, item::setDbCostMin);
                    if (reqCount > 0L) {
                        item.setAvg(1.0D * reqTotalDelay / reqCount);
                        item.setDbCostAvg(1.0D * dbCostTotal / reqCount);
                    }

                    if (delays.size() >= maxDepth) {
                        item.setP95(ApiMetricsDelayUtil.p95(mergedDelay, reqCount));
                        item.setP99(ApiMetricsDelayUtil.p99(mergedDelay, reqCount));
                        item.setDbCostP95(ApiMetricsDelayUtil.p95(mergedDBCost, reqCount));
                        item.setDbCostP95(ApiMetricsDelayUtil.p99(mergedDBCost, reqCount));
                    }
                }
        );
        for (ServerChart.Item item : items) {
            if (item.getTs() < param.getStartAt() || item.getTs() >= param.getEndAt()) {
                continue;
            }
            result.add(item);
        }
        return result;
    }

    protected long errorCountGetter(List<ApiMetricsRaw> rows, LongConsumer acceptReqCount) {
        long errorCount = 0L;
        for (final ApiMetricsRaw info : rows) {
            if (null == info) {
                continue;
            }
            acceptReqCount.accept(Optional.ofNullable(info.getReqCount()).orElse(0L));
            errorCount += Optional.ofNullable(info.getErrorCount()).orElse(0L);
        }
        return errorCount;
    }

    public List<TopApiInServer> topApiInServer(TopApiInServerParam param) {
        String serverId = param.getServerId();
        if (StringUtils.isBlank(serverId)) {
            throw new BizException(SERVER_ID_EMPTY);
        }
        Criteria criteria = ParticleSizeAnalyzer.of(param);
        criteria.and(PROCESS_ID).is(serverId);
        Query query = Query.query(criteria);
        List<ApiMetricsRaw> apiMetricsRaws = service.supplementMetricsRaw(service.find(query), param, c -> c.and(PROCESS_ID).is(serverId), Criteria.where("api_gateway_uuid").is(serverId));
        Map<String, TopApiInServer> apiInfoMap = apiMetricsRaws.stream()
                .filter(Objects::nonNull)
                .filter(e -> StringUtils.isNotBlank(e.getApiId()))
                .collect(
                        Collectors.groupingBy(
                                ApiMetricsRaw::getApiId,
                                Collectors.collectingAndThen(Collectors.toList(), rows -> {
                                    TopApiInServer item = TopApiInServer.create();
                                    item.setQueryFrom(param.getStartAt());
                                    item.setQueryEnd(param.getEndAt());
                                    item.setGranularity(param.getGranularity());
                                    long errorCount = errorCountGetter(rows, e -> item.setRequestCount(item.getRequestCount() + e));
                                    int total = item.getRequestCount().intValue();
                                    if (total > 0) {
                                        item.setErrorRate(ApiMetricsDelayInfoUtil.rate(errorCount, item.getRequestCount()));
                                        item.setErrorCount(errorCount);
                                        baseDataCalculate(item, apiMetricsRaws, null);
                                    }
                                    return item;
                                })
                        ));
        List<ObjectId> apiIds = apiMetricsRaws.stream()
                .map(ApiMetricsRaw::getApiId)
                .distinct()
                .map(MongoUtils::toObjectId)
                .filter(Objects::nonNull)
                .toList();
        apiInfoMap.forEach((apiId, apiInfo) -> {
            apiInfo.setApiId(apiId);
            apiInfo.setApiName(apiId);
            apiInfo.setApiPath(apiId);
            apiInfo.setNotExistsApi(true);
        });
        if (CollectionUtils.isEmpty(apiIds)) {
            return TopApiInServer.supplement(new ArrayList<>(), publishApis());
        }
        Criteria criteriaOfApi = Criteria.where("_id").in(apiIds);
        Query queryOfApi = Query.query(criteriaOfApi);
        List<ModulesDto> apiDtoList = modulesService.findAll(queryOfApi);
        apiDtoList.forEach(apiDto -> {
            String apiId = apiDto.getId().toHexString();
            TopApiInServer item = apiInfoMap.computeIfAbsent(apiId, k -> new TopApiInServer());
            String path = ApiPathUtil.apiPath(apiDto.getApiVersion(), apiDto.getBasePath(), apiDto.getPrefix());
            item.setApiId(apiId);
            item.setApiName(apiDto.getName());
            item.setApiPath(path);
            item.setNotExistsApi(false);
        });
        List<TopApiInServer> result = new ArrayList<>(apiInfoMap.values());
        TopApiInServer.supplement(result, publishApis());
        ChartSortUtil.sort(result, param.getSortInfo(), TopApiInServer.class);
        return result;
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
        ParticleSizeAnalyzer.of(param);
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
        List<? extends ServerUsage> allUsage = queryCpuUsageRecords(criteriaOfUsage, param.getStartAt(), param.getEndAt(), param.getGranularity());
        Map<String, ServerChart.Usage> usageMap = allUsage.stream()
                .filter(Objects::nonNull)
                .filter(e -> StringUtils.isNotBlank(e.getWorkOid()))
                .collect(Collectors.groupingBy(ServerUsage::getWorkOid, Collectors.collectingAndThen(
                        Collectors.toList(),
                        items -> this.mapUsage(items, param.getStartAt(), param.getEndAt(), param.getGranularity())
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
        result.setGranularity(param.getGranularity());
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

    public ApiTopOnHomepage apiTopOnHomepage(QueryBase param) {
        //@todo db cost
        ApiTopOnHomepage result = new ApiTopOnHomepage();
        Criteria criteria = ParticleSizeAnalyzer.of(result, param);
        Query query = Query.query(criteria);
        List<ApiMetricsRaw> apiMetricsRaws = service.supplementMetricsRaw(service.find(query), param, c -> {
        }, null);
        if (CollectionUtils.isEmpty(apiMetricsRaws)) {
            return result;
        }
        long apiCount = apiMetricsRaws.stream().map(ApiMetricsRaw::getApiId).distinct().count();
        long totalRequestCount = apiMetricsRaws.stream().mapToLong(ApiMetricsRaw::getReqCount).sum();
        long totalBytes = apiMetricsRaws.stream().map(ApiMetricsRaw::getBytes).map(ApiMetricsDelayUtil::fixDelayAsMap).mapToLong(ApiMetricsDelayUtil::sum).sum();
        long totalDelayMs = apiMetricsRaws.stream().map(ApiMetricsRaw::getDelay).map(ApiMetricsDelayUtil::fixDelayAsMap).mapToLong(ApiMetricsDelayUtil::sum).sum();
        result.setTotalBytes(totalBytes);
        result.setTotalDelayMs(totalDelayMs);
        result.setApiCount(apiCount);
        result.setTotalRequestCount(totalRequestCount);
        result.setTotalRps(totalDelayMs > 0L ? totalBytes * 1000.0D / totalDelayMs : 0D);
        result.setResponseTimeAvg(totalRequestCount > 0L ? totalDelayMs * 1.0D / totalRequestCount : 0D);
        return result;
    }

    public List<ApiItem> apiOverviewList(ApiListParam param) {
        Criteria criteria = ParticleSizeAnalyzer.of(param);
        Query query = Query.query(criteria);
        List<ApiMetricsRaw> apiMetricsRaws = service.supplementMetricsRaw(service.find(query), param, c -> {
        }, null);
        if (CollectionUtils.isEmpty(apiMetricsRaws)) {
            return ApiItem.supplement(new ArrayList<>(), publishApis());
        }
        List<ObjectId> apiIds = apiMetricsRaws.stream()
                .filter(Objects::nonNull)
                .map(ApiMetricsRaw::getApiId)
                .filter(StringUtils::isNotBlank)
                .distinct()
                .map(MongoUtils::toObjectId)
                .filter(Objects::nonNull)
                .toList();
        if (apiIds.isEmpty()) {
            return ApiItem.supplement(new ArrayList<>(), publishApis());
        }
        Criteria criteriaOfApi = Criteria.where("_id").in(apiIds);
        Query queryApiInfo = Query.query(criteriaOfApi);
        List<ModulesDto> allApi = modulesService.findAll(queryApiInfo);
        Map<String, ApiItem> apiMap = allApi.stream().collect(Collectors.toMap(e -> e.getId().toHexString(), e -> {
            ApiItem item = new ApiItem();
            item.setApiId(e.getId().toHexString());
            String path = ApiPathUtil.apiPath(e.getApiVersion(), e.getBasePath(), e.getPrefix());
            item.setApiPath(path);
            item.setApiName(e.getName());
            item.setNotExistsApi(false);
            return item;
        }, (e1, e2) -> e2));
        Map<String, ApiItem> collect = apiMetricsRaws.stream()
                .filter(Objects::nonNull)
                .filter(e -> StringUtils.isNotBlank(e.getApiId()))
                .collect(
                        Collectors.groupingBy(ApiMetricsRaw::getApiId,
                                Collectors.collectingAndThen(
                                        Collectors.toList(),
                                        rows -> {
                                            ApiMetricsRaw apiMetricsRaw = rows.get(0);
                                            ApiItem item = Optional.ofNullable(apiMap.get(apiMetricsRaw.getApiId())).orElse(new ApiItem());
                                            long sumRequestCount = rows.stream()
                                                    .filter(Objects::nonNull)
                                                    .mapToLong(ApiMetricsRaw::getReqCount)
                                                    .sum();
                                            item.setRequestCount(sumRequestCount);
                                            long sumErrorCount = rows.stream()
                                                    .filter(Objects::nonNull)
                                                    .mapToLong(ApiMetricsRaw::getErrorCount)
                                                    .sum();
                                            item.setErrorRate(ApiMetricsDelayInfoUtil.rate(sumErrorCount, item.getRequestCount()));
                                            item.setErrorCount(sumErrorCount);
                                            long sumRps = rows.stream()
                                                    .filter(Objects::nonNull)
                                                    .map(ApiMetricsRaw::getBytes)
                                                    .map(ApiMetricsDelayUtil::sum)
                                                    .mapToLong(Long::longValue).sum();
                                            baseDataCalculate(item, apiMetricsRaws, sumDelay -> item.setTotalRps(sumDelay > 0 ? 1000.0D * sumRps / sumDelay : 0D));
                                            item.setQueryFrom(param.getStartAt());
                                            item.setQueryEnd(param.getEndAt());
                                            item.setGranularity(param.getGranularity());
                                            return item;
                                        })));
        collect.forEach((apiId, apiInfo) -> {
            if (StringUtils.isBlank(apiInfo.getApiId())) {
                apiInfo.setApiId(apiId);
                apiInfo.setApiName(apiId);
                apiInfo.setApiPath(apiId);
                apiInfo.setNotExistsApi(true);
            }
        });
        List<ApiItem> result = new ArrayList<>(collect.values());
        ApiItem.supplement(result, publishApis());
        ChartSortUtil.sort(result, param.getSortInfo(), ApiItem.class);
        return result;
    }

    public ApiDetail apiOverviewDetail(ApiDetailParam param) {
        ApiDetail result = new ApiDetail();
        Criteria criteria = ParticleSizeAnalyzer.of(result, param);
        List<ApiMetricsRaw> apiMetricsRaws = findRowByApiId(criteria, param.getApiId(), param);
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
            baseDataCalculate(result, apiMetricsRaws, null);
        }
        return result;
    }

    protected List<ApiMetricsRaw> findRowByApiId(Criteria criteria, String apiId, QueryBase param) {
        return findRowByApiId(criteria, apiId, param, true);
    }

    protected List<ApiMetricsRaw> findRowByApiId(Criteria criteria, String apiId, QueryBase param, boolean filterByTime) {
        if (StringUtils.isBlank(apiId)) {
            throw new BizException("api.id.empty");
        }
        criteria.and("apiId").is(apiId);
        return service.supplementMetricsRaw(service.find(Query.query(criteria)), param, filterByTime, c -> c.and("apiId").is(apiId), Criteria.where("allPathId").is(apiId));
    }

    public List<ApiOfEachServer> apiOfEachServer(ApiWithServerDetail param) {
        Criteria criteria = ParticleSizeAnalyzer.of(param);
        List<ApiMetricsRaw> apiMetricsRaws = findRowByApiId(criteria, param.getApiId(), param);
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
                                            baseDataCalculate(item, apiMetricsRaws, null);
                                            item.setQueryFrom(param.getStartAt());
                                            item.setQueryEnd(param.getEndAt());
                                            item.setGranularity(param.getGranularity());
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
        Criteria criteria = ParticleSizeAnalyzer.of(result, param);
        List<ApiMetricsRaw> apiMetricsRaws = findRowByApiId(criteria, param.getApiId(), param, false);
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
                                            long totalBytes = rows.stream().map(ApiMetricsRaw::getBytes).map(ApiMetricsDelayUtil::fixDelayAsMap).mapToLong(ApiMetricsDelayUtil::sum).sum();
                                            List<Map<Long, Integer>> mergedDelay = ApiMetricsDelayInfoUtil.mergeItems(rows, ApiMetricsRaw::getDelay);
                                            item.setDelay(mergedDelay);
                                            item.setTs(timeStart);
                                            item.setTotalBytes(totalBytes);
                                            List<Map<Long, Integer>> mergedDBCost = ApiMetricsDelayInfoUtil.mergeItems(rows, ApiMetricsRaw::getDbCost);
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
            case 0, 1 -> 60;
            default -> 1;
        };
        List<List<Map<Long, Integer>>> delays = new ArrayList<>();
        List<Long> bytes = new ArrayList<>();
        List<List<Map<Long, Integer>>> dbCosts = new ArrayList<>();
        List<ChartAndDelayOfApi.Item> items = ChartSortUtil.fixAndSort(collect,
                param.getQStart(), param.getEndAt(), param.getGranularity(),
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
                    List<Map<Long, Integer>> mergeDdCosts = ApiMetricsDelayUtil.merge(dbCosts);
                    Long totalDbCost = ApiMetricsDelayUtil.sum(mergeDdCosts);
                    ApiMetricsDelayUtil.readMaxAndMin(mergeDdCosts, item::setDbCostMax, item::setDbCostMin);
                    List<Map<Long, Integer>> mergedDelays = ApiMetricsDelayUtil.merge(delays);
                    Long totalDelayMs = ApiMetricsDelayUtil.sum(mergedDelays);
                    Long reqCount = ApiMetricsDelayUtil.sum(mergedDelays, (iKey, iCount) -> iCount.longValue());
                    if (reqCount > 0L) {
                        item.setRequestCostAvg(1.0D * totalDelayMs / reqCount);
                        item.setDbCostAvg(1.0D * totalDbCost / reqCount);
                    } else {
                        item.setRequestCostAvg(0D);
                        item.setDbCostAvg(0D);
                    }
                    ApiMetricsDelayUtil.readMaxAndMin(mergedDelays, item::setMaxDelay, item::setMinDelay);
                    long totalBytes = bytes.stream().filter(Objects::nonNull).mapToLong(Long::longValue).sum();
                    item.setRps(totalDelayMs > 0L ? (totalBytes * 1000.0D / totalDelayMs) : 0D);
                    if (delays.size() >= maxDepth) {
                        item.setP95(ApiMetricsDelayUtil.p95(mergedDelays, reqCount));
                        item.setP99(ApiMetricsDelayUtil.p99(mergedDelays, reqCount));
                        item.setDbCostP95(ApiMetricsDelayUtil.p95(mergeDdCosts, reqCount));
                        item.setDbCostP99(ApiMetricsDelayUtil.p99(mergeDdCosts, reqCount));
                    }
                });
        for (ChartAndDelayOfApi.Item item : items) {
            if (item.getTs() < param.getStartAt() || item.getTs() >= param.getEndAt()) {
                continue;
            }
            result.add(item);
        }
        return result;
    }

    protected Map<String, ModulesDto> publishApis() {
        List<ModulesDto> allActiveApi = (List<ModulesDto>) modulesService.findAllActiveApi(ModuleStatusEnum.ACTIVE);
        return allActiveApi.stream()
                .filter(Objects::nonNull)
                .filter(e -> Objects.nonNull(e.getId()))
                .collect(Collectors.toMap(e -> e.getId().toHexString(), e -> e, (e1, e2) -> e1));
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

    <T extends ValueBase> void baseDataCalculate(T item, List<ApiMetricsRaw> apiMetricsRaws, LongConsumer valueSetter) {
        if (item instanceof DataValueBase result) {
            ApiMetricsDelayInfoUtil.Setter delaySetter = ApiMetricsDelayInfoUtil.Setter.of(valueSetter)
                    .avg(result::setResponseTimeAvg)
                    .max(result::setMaxDelay)
                    .min(result::setMinDelay)
                    .p95(result::setP95)
                    .p99(result::setP99);
            ApiMetricsDelayInfoUtil.calculate(apiMetricsRaws, ApiMetricsRaw::getDelay, delaySetter);
            ApiMetricsDelayInfoUtil.Setter dbCostSetter = ApiMetricsDelayInfoUtil.Setter.of(result::setDbCostTotal)
                    .avg(result::setDbCostAvg)
                    .max(result::setDbCostMax)
                    .min(result::setDbCostMin)
                    .p95(result::setDbCostP95)
                    .p99(result::setDbCostP99);
            ApiMetricsDelayInfoUtil.calculate(apiMetricsRaws, ApiMetricsRaw::getDbCost, dbCostSetter);
        }
    }
}
