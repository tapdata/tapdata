package com.tapdata.tm.v2.api.monitor.service;

import com.tapdata.tm.apiCalls.service.WorkerCallServiceImpl;
import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.cluster.dto.Component;
import com.tapdata.tm.cluster.entity.ClusterStateEntity;
import com.tapdata.tm.cluster.repository.ClusterStateRepository;
import com.tapdata.tm.module.dto.ModulesDto;
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
import com.tapdata.tm.v2.api.monitor.main.dto.TopWorkerInServer;
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
import com.tapdata.tm.v2.api.monitor.utils.ApiMetricsDelayUtil;
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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
    static final String PATH_SPLIT = "/";
    public static final String WORKER_TYPE = "worker_type";
    public static final String PROCESS_ID = "processId";
    public static final String PROCESS_TYPE = "processType";
    public static final String API_SERVER = "api-server";
    public static final String REQUEST_COUNT = "requestCount";
    public static final String ERROR_RATE = "errorRate";
    ApiMetricsRawService service;
    UsageRepository usageRepository;
    WorkerRepository workerRepository;
    ClusterStateRepository clusterRepository;
    ModulesService modulesService;
    MongoTemplate mongoTemplate;
    ServerUsageMetricRepository serverUsageMetricRepository;

    public ServerTopOnHomepage serverTopOnHomepage(QueryBase param) {
        final ServerTopOnHomepage result = ServerTopOnHomepage.create();
        final Criteria criteria = ParticleSizeAnalyzer.of(result, param);
        final Query query = Query.query(criteria);
        final List<ApiMetricsRaw> apiMetricsRaws = ParticleSizeAnalyzer.apiMetricsRaws(service.find(query), param);
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
            final List<Map<Long, Integer>> merge = mergeDelay(apiMetricsRaws);
            final long responseTime = ApiMetricsDelayUtil.sum(merge);
            final Long p95 = ApiMetricsDelayUtil.p95(merge, totalRequestCount);
            final Long p99 = ApiMetricsDelayUtil.p99(merge, totalRequestCount);
            final long errorServer = errorCount(apiMetricsRaws, ApiMetricsRaw::getProcessId);
            final long errorApi = errorCount(apiMetricsRaws, ApiMetricsRaw::getApiId);
            result.setP95(p95);
            result.setP99(p99);
            ApiMetricsDelayUtil.readMaxAndMin(merge, result::setMaxDelay, result::setMinDelay);
            result.setResponseTime(responseTime);
            result.setResponseTimeAvg(responseTime * 1.0D / totalRequestCount);
            result.setErrorCount(errorCount.get());
            result.setTotalErrorRate(errorCount.get() * 1.0D / totalRequestCount);
            result.setNotHealthyApiCount(errorApi);
            result.setNotHealthyServerCount(errorServer);
        }
        return result;
    }

    protected long errorCount(List<ApiMetricsRaw> apiMetricsRaws, Function<ApiMetricsRaw, String> groupBy) {
        return apiMetricsRaws.stream()
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
            return result;
        }
        if (StringUtils.isNotBlank(serverMatchName)) {
            criteria.and(PROCESS_ID).in(serverMap.keySet());
        }

        final Query query = Query.query(criteria);
        final List<ApiMetricsRaw> apiMetricsRaws = ParticleSizeAnalyzer.apiMetricsRaws(service.find(query), param);

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
                .collect(Collectors.groupingBy(ServerUsage::getProcessId, Collectors.collectingAndThen(
                        Collectors.toList(),
                        items -> this.mapUsage(items, param.getStartAt(), param.getEndAt(), param.getGranularity())
                )));
        final Map<String, ServerItem> collect = apiMetricsRaws.stream().collect(Collectors.groupingBy(
                ApiMetricsRaw::getProcessId,
                Collectors.collectingAndThen(
                        Collectors.toList(),
                        infos -> {
                            final ServerItem item = ServerItem.create();
                            long errorCount = errorCountGetter(infos, e -> item.setRequestCount(item.getRequestCount() + e));
                            int total = item.getRequestCount().intValue();
                            if (total > 0) {
                                item.setErrorRate(1.0D * errorCount / item.getRequestCount());
                                final List<Map<Long, Integer>> merge = mergeDelay(infos);
                                final Long p95 = ApiMetricsDelayUtil.p95(merge, total);
                                final Long p99 = ApiMetricsDelayUtil.p99(merge, total);
                                ApiMetricsDelayUtil.readMaxAndMin(merge, item::setMaxDelay, item::setMinDelay);
                                item.setP95(p95);
                                item.setP99(p99);
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
        result.sort(Comparator.comparing(ServerItem::getServerName));
        return result;
    }

    protected <T extends ServerUsage> List<T> queryCpuUsageRecords(Criteria criteriaBase, long queryStart, long queryEnd, int type) {
        criteriaBase.andOperator(
                Criteria.where("lastUpdateTime").gte(queryStart * 1000L),
                Criteria.where("lastUpdateTime").lte(queryEnd * 1000L)
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
        if (infos.isEmpty()) {
            return usage;
        }

        // Sort by time
        infos.sort(Comparator.comparingLong(ServerUsage::getLastUpdateTime));

        // Calculate step based on granularity
        long step = switch (granularity) {
            case 1 -> 60L;           // 1 minute
            case 2 -> 60L * 60L;    // 1 hour
            default -> 5L;          // 5 seconds (default)
        };

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
            throw new BizException("server.id.empty");
        }
        ServerOverviewDetail result = new ServerOverviewDetail();
        Worker worker = findServerById(serverId);
        result.setServerName(Optional.ofNullable(worker.getHostname()).orElse(""));
        result.setServerId(serverId);
        Optional.ofNullable(worker.getWorkerStatus())
                .map(ApiServerStatus::getMetricValues)
                .ifPresent(u -> {
                    result.setCpuUsage(u.getCpuUsage());
                    result.setMemoryUsage(u.getHeapMemoryUsage());
                    if (u.getLastUpdateTime() instanceof Number iNum) {
                        result.setUsagePingTime(iNum.longValue());
                    }
                });
        Criteria criteria = ParticleSizeAnalyzer.of(result, param);
        final Query query = Query.query(criteria);
        final List<ApiMetricsRaw> apiMetricsRaws = ParticleSizeAnalyzer.apiMetricsRaws(service.find(query), param);
        result.setRequestCount(0L);
        long errorCount = errorCountGetter(apiMetricsRaws, e -> result.setRequestCount(result.getRequestCount() + e));
        result.setErrorRate(1.0D * errorCount / result.getRequestCount());
        final List<Map<Long, Integer>> merge = mergeDelay(apiMetricsRaws);
        result.setP95(ApiMetricsDelayUtil.p95(merge, result.getRequestCount()));
        result.setP99(ApiMetricsDelayUtil.p99(merge, result.getRequestCount().intValue()));
        ApiMetricsDelayUtil.readMaxAndMin(merge, result::setMaxDelay, result::setMinDelay);
        return result;
    }

    public ServerChart serverChart(ServerChartParam param) {
        String serverId = param.getServerId();
        if (StringUtils.isBlank(serverId)) {
            throw new BizException("server.id.empty");
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
        List<ApiMetricsRaw> apiMetricsRaws = ParticleSizeAnalyzer.apiMetricsRaws(service.find(query), param);
        result.setRequest(ServerChart.Request.create());
        result.setDelay(ServerChart.Delay.create());
        Map<Long, ServerChart.Item> collect = apiMetricsRaws.stream().collect(
                Collectors.groupingBy(
                        ApiMetricsRaw::getTimeStart,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                rows -> {
                                    ServerChart.Item item = new ServerChart.Item();
                                    ApiMetricsRaw apiMetricsRaw = rows.get(0);
                                    long totalErrorCount = rows.stream().mapToLong(ApiMetricsRaw::getErrorCount).sum();
                                    long reqCount = rows.stream().mapToLong(ApiMetricsRaw::getReqCount).sum();

                                    List<Map<Long, Integer>> maps = mergeDelay(rows);
                                    long delaySum = ApiMetricsDelayUtil.sum(maps);
                                    Long p95 = ApiMetricsDelayUtil.p95(maps, reqCount);
                                    Long p99 = ApiMetricsDelayUtil.p99(maps, reqCount);
                                    ApiMetricsDelayUtil.readMaxAndMin(maps, item::setMaxDelay, item::setMinDelay);
                                    item.setTs(apiMetricsRaw.getTimeStart());
                                    item.setRequestCount(reqCount);
                                    item.setErrorRate(reqCount > 0L ? (1.0D * totalErrorCount / reqCount) : 0D);
                                    item.setAvg(reqCount > 0L ? (1.0D * delaySum / reqCount) : 0D);
                                    item.setP95(p95);
                                    item.setP99(p99);
                                    return item;
                                }
                        )
                )
        );

        // fix time and sort by time
        ChartSortUtil.fixAndSort(collect,
                param.getStartAt(), param.getEndAt(), param.getGranularity(),
                ServerChart.Item::create,
                item -> {
                    result.getRequest().getRequestCount().add(item.getRequestCount());
                    result.getRequest().getErrorRate().add(item.getErrorRate());
                    result.getRequest().getTs().add(item.getTs());
                    result.getDelay().getTs().add(item.getTs());
                    result.getDelay().getAvg().add(item.getAvg());
                    result.getDelay().getP95().add(item.getP95());
                    result.getDelay().getP99().add(item.getP99());
                    result.getDelay().getMaxDelay().add(item.getMaxDelay());
                    result.getDelay().getMinDelay().add(item.getMinDelay());
                }
        );
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
            throw new BizException("server.id.empty");
        }
        Criteria criteria = ParticleSizeAnalyzer.of(param);
        criteria.and(PROCESS_ID).is(serverId);
        Query query = Query.query(criteria);
        List<ApiMetricsRaw> apiMetricsRaws = ParticleSizeAnalyzer.apiMetricsRaws(service.find(query), param);
        Map<String, TopApiInServer> apiInfoMap = apiMetricsRaws.stream().collect(
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
                                item.setErrorRate(1.0D * errorCount / item.getRequestCount());
                                final List<Map<Long, Integer>> merge = mergeDelay(rows);
                                final Long p99 = ApiMetricsDelayUtil.p99(merge, total);
                                ApiMetricsDelayUtil.readMaxAndMin(merge, item::setMaxDelay, item::setMinDelay);
                                item.setP99(p99);
                                item.setAvg(1.0D * ApiMetricsDelayUtil.sum(merge) / total);
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
        if (CollectionUtils.isEmpty(apiIds)) {
            return new ArrayList<>();
        }
        Criteria criteriaOfApi = Criteria.where("_id").in(apiIds);
        Query queryOfApi = Query.query(criteriaOfApi);
        List<ModulesDto> apiDtoList = modulesService.findAll(queryOfApi);
        apiDtoList.forEach(apiDto -> {
            String apiId = apiDto.getId().toHexString();
            TopApiInServer item = Optional.ofNullable(apiInfoMap.get(apiId)).orElse(new TopApiInServer());
            item.setApiId(apiId);
            item.setApiName(apiDto.getName());
        });
        List<TopApiInServer> result = new ArrayList<>(apiInfoMap.values());
        String orderBy = param.getOrderBy();
        if (StringUtils.isBlank(orderBy)) {
            orderBy = REQUEST_COUNT;
        }
        orderBy = orderBy.trim();
        Comparator<TopApiInServer> comparing = switch (orderBy) {
            case ERROR_RATE -> (e1, e2) -> {
                Double p1 = Optional.ofNullable(e1.getErrorRate()).orElse(0D);
                Double p2 = Optional.ofNullable(e2.getErrorRate()).orElse(0D);
                return p1.compareTo(p2);
            };
            case "avg" -> (e1, e2) -> {
                Double p1 = Optional.ofNullable(e1.getAvg()).orElse(0D);
                Double p2 = Optional.ofNullable(e2.getAvg()).orElse(0D);
                return p1.compareTo(p2);
            };
            case "p99" -> (e1, e2) -> {
                Long p1 = Optional.ofNullable(e1.getP99()).orElse(0L);
                Long p2 = Optional.ofNullable(e2.getP99()).orElse(0L);
                return p1.compareTo(p2);
            };
            default -> (e1, e2) -> {
                Long p1 = Optional.ofNullable(e1.getRequestCount()).orElse(0L);
                Long p2 = Optional.ofNullable(e2.getRequestCount()).orElse(0L);
                return p1.compareTo(p2);
            };
        };
        result.sort(comparing.reversed());
        return result;
    }

    public TopWorkerInServer topWorkerInServer(TopWorkerInServerParam param) {
        String serverId = param.getServerId();
        if (StringUtils.isBlank(serverId)) {
            throw new BizException("server.id.empty");
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
        List<String> workerIds = workers.stream().
                filter(Objects::nonNull)
                .map(ApiServerWorkerInfo::getOid)
                .filter(StringUtils::isNotBlank)
                .toList();
        if (workerIds.isEmpty()) {
            return result;
        }
        long endAt = param.getEndAt() * 1000L;
        if (endAt % 60000L != 0L) {
            endAt = (endAt + 60000L) / 60000L * 60000L;
        }
        Criteria criteriaOfWorker = Criteria.where(WorkerCallServiceImpl.Tag.TIME_GRANULARITY).is(1)
                .and(WorkerCallServiceImpl.Tag.DELETE).ne(true)
                .and(WorkerCallServiceImpl.Tag.PROCESS_ID).is(serverId)
                .andOperator(
                        Criteria.where(WorkerCallServiceImpl.Tag.TIME_START).gte(param.getStartAt() * 1000L),
                        Criteria.where(WorkerCallServiceImpl.Tag.TIME_START).lte(endAt)
                );
        List<WorkerCallEntity> callOfWorker = mongoTemplate.find(Query.query(criteriaOfWorker), WorkerCallEntity.class, "WorkerCalls");
        Map<String, TopWorkerInServer.TopWorkerInServerItem> workerInfoMap = callOfWorker.stream().collect(Collectors.groupingBy(
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
                            item.setErrorRate(reqCount > 0L ? (1.0D * errorCount / reqCount) : 0D);
                            return item;
                        }
                )
        ));
        Criteria criteriaOfUsage = Criteria.where(PROCESS_ID).is(serverId)
                .and(PROCESS_TYPE).is(ServerUsage.ProcessType.API_SERVER.getType());
        List<? extends ServerUsage> allUsage = queryCpuUsageRecords(criteriaOfUsage, param.getStartAt(), param.getEndAt(), param.getGranularity());
        Map<String, ServerChart.Usage> usageMap = allUsage.stream()
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
        result.getWorkerList().sort(Comparator.comparing(TopWorkerInServer.TopWorkerInServerItem::getWorkerName));
        return result;
    }

    public ApiTopOnHomepage apiTopOnHomepage(QueryBase param) {
        ApiTopOnHomepage result = new ApiTopOnHomepage();
        Criteria criteria = ParticleSizeAnalyzer.of(result, param);
        Query query = Query.query(criteria);
        List<ApiMetricsRaw> apiMetricsRaws = ParticleSizeAnalyzer.apiMetricsRaws(service.find(query), param);
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
        result.setTotalRps(totalDelayMs > 0L ? totalBytes * 1000.0D / totalDelayMs : 0D);
        result.setResponseTimeAvg(totalRequestCount > 0L ? totalDelayMs * 1.0D / totalRequestCount : 0D);
        return result;
    }

    public List<ApiItem> apiOverviewList(ApiListParam param) {
        Criteria criteria = ParticleSizeAnalyzer.of(param);
        Query query = Query.query(criteria);
        List<ApiMetricsRaw> apiMetricsRaws = ParticleSizeAnalyzer.apiMetricsRaws(service.find(query), param);
        if (CollectionUtils.isEmpty(apiMetricsRaws)) {
            return new ArrayList<>();
        }
        List<String> apiIds = apiMetricsRaws.stream().map(ApiMetricsRaw::getApiId).distinct().toList();
        if (apiIds.isEmpty()) {
            return new ArrayList<>();
        }
        Criteria criteriaOfApi = Criteria.where("_id").in(apiIds);
        Query queryApiInfo = Query.query(criteriaOfApi);
        List<ModulesDto> allApi = modulesService.findAll(queryApiInfo);
        Map<String, ApiItem> apiMap = allApi.stream().collect(Collectors.toMap(e -> e.getId().toHexString(), e -> {
            ApiItem item = new ApiItem();
            item.setApiId(e.getId().toHexString());
            String apiVersion = StringUtils.isBlank(e.getApiVersion()) ? "" : (PATH_SPLIT + e.getApiVersion());
            String apiBasePath = StringUtils.isBlank(e.getBasePath()) ? "" : (PATH_SPLIT + e.getBasePath());
            String apiPrefix = StringUtils.isBlank(e.getPrefix()) ? "" : (PATH_SPLIT + e.getPrefix());
            item.setApiPath(apiVersion + apiPrefix + apiBasePath);
            item.setApiName(e.getName());
            return item;
        }, (e1, e2) -> e2));

        Map<String, ApiItem> collect = apiMetricsRaws.stream().collect(
                Collectors.groupingBy(ApiMetricsRaw::getApiId,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                rows -> {
                                    ApiMetricsRaw apiMetricsRaw = rows.get(0);
                                    ApiItem item = Optional.ofNullable(apiMap.get(apiMetricsRaw.getApiId())).orElse(new ApiItem());
                                    item.setRequestCount(rows.stream().mapToLong(ApiMetricsRaw::getReqCount).sum());
                                    item.setTotalRps(rows.stream().mapToDouble(ApiMetricsRaw::getRps).sum());
                                    item.setErrorRate(rows.stream().mapToLong(ApiMetricsRaw::getErrorCount).sum() * 1.0D / item.getRequestCount());
                                    item.setRequestCostAvg(rows.stream().map(ApiMetricsRaw::getDelay).map(ApiMetricsDelayUtil::fixDelayAsMap).mapToLong(ApiMetricsDelayUtil::sum).sum() * 1.0D / item.getRequestCount());
                                    final List<Map<Long, Integer>> merged = mergeDelay(rows);
                                    item.setP95(ApiMetricsDelayUtil.p95(merged, item.getRequestCount()));
                                    item.setP99(ApiMetricsDelayUtil.p99(merged, item.getRequestCount()));
                                    ApiMetricsDelayUtil.readMaxAndMin(merged, item::setMaxDelay, item::setMinDelay);
                                    item.setQueryFrom(param.getStartAt());
                                    item.setQueryEnd(param.getEndAt());
                                    item.setGranularity(param.getGranularity());
                                    return item;
                                })));
        List<ApiItem> result = new ArrayList<>(collect.values());
        String orderBy = param.getOrderBy();
        if (StringUtils.isBlank(orderBy)) {
            orderBy = REQUEST_COUNT;
        }
        orderBy = orderBy.trim();
        switch (orderBy) {
            case "requestCostAvg":
                result.sort(Comparator.comparing(ApiItem::getRequestCostAvg));
                break;
            case "p95":
                result.sort((e1, e2) -> {
                    Long p1 = Optional.ofNullable(e1.getP95()).orElse(0L);
                    Long p2 = Optional.ofNullable(e2.getP95()).orElse(0L);
                    return p1.compareTo(p2);
                });
                break;
            case "p99":
                result.sort((e1, e2) -> {
                    Long p1 = Optional.ofNullable(e1.getP99()).orElse(0L);
                    Long p2 = Optional.ofNullable(e2.getP99()).orElse(0L);
                    return p1.compareTo(p2);
                });
                break;
            case ERROR_RATE:
                result.sort(Comparator.comparing(ApiItem::getErrorRate));
                break;
            case "totalRps":
                result.sort(Comparator.comparing(ApiItem::getTotalRps));
                break;
            default:
                result.sort(Comparator.comparing(ApiItem::getRequestCount));
        }
        result.sort(Comparator.comparing(ApiItem::getRequestCount));
        return result;
    }

    public ApiDetail apiOverviewDetail(ApiDetailParam param) {
        ApiDetail result = new ApiDetail();
        Criteria criteria = ParticleSizeAnalyzer.of(result, param);
        List<ApiMetricsRaw> apiMetricsRaws = findRowByApiId(criteria, param.getApiId(), param);
        if (!CollectionUtils.isEmpty(apiMetricsRaws)) {
            long totalRequestCount = apiMetricsRaws.stream().mapToLong(ApiMetricsRaw::getReqCount).sum();
            long totalErrorCount = apiMetricsRaws.stream().mapToLong(ApiMetricsRaw::getErrorCount).sum();
            long totalDelayMs = apiMetricsRaws.stream().map(ApiMetricsRaw::getDelay).map(ApiMetricsDelayUtil::fixDelayAsMap).mapToLong(ApiMetricsDelayUtil::sum).sum();
            final List<Map<Long, Integer>> merge = mergeDelay(apiMetricsRaws);
            result.setP95(ApiMetricsDelayUtil.p95(merge, totalRequestCount));
            result.setP99(ApiMetricsDelayUtil.p99(merge, totalRequestCount));
            ApiMetricsDelayUtil.readMaxAndMin(merge, result::setMaxDelay, result::setMinDelay);
            result.setRequestCount(totalRequestCount);
            result.setErrorRate(totalRequestCount > 0L ? (totalErrorCount * 1.0D / totalRequestCount) : 0D);
            result.setRequestCostAvg(totalRequestCount > 0L ? (1.0D * totalDelayMs / totalRequestCount) : 0L);
        }
        return result;
    }

    protected List<ApiMetricsRaw> findRowByApiId(Criteria criteria, String apiId, QueryBase param) {
        if (StringUtils.isBlank(apiId)) {
            throw new BizException("api.id.empty");
        }
        criteria.and("apiId").is(apiId);
        return ParticleSizeAnalyzer.apiMetricsRaws(service.find(Query.query(criteria)), param);
    }

    public List<ApiOfEachServer> apiOfEachServer(ApiWithServerDetail param) {
        Criteria criteria = ParticleSizeAnalyzer.of(param);
        List<ApiMetricsRaw> apiMetricsRaws = findRowByApiId(criteria, param.getApiId(), param);
        if (apiMetricsRaws.isEmpty()) {
            return new ArrayList<>();
        }
        List<String> serverIds = apiMetricsRaws.stream().map(ApiMetricsRaw::getProcessId).distinct().toList();
        if (serverIds.isEmpty()) {
            return new ArrayList<>();
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
        Map<String, ApiOfEachServer> collect = apiMetricsRaws.stream().collect(
                Collectors.groupingBy(
                        ApiMetricsRaw::getProcessId,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                rows -> {
                                    ApiMetricsRaw first = rows.get(0);
                                    String processId = first.getProcessId();
                                    ApiOfEachServer item = Optional.ofNullable(serverMap.get(processId)).orElse(new ApiOfEachServer());
                                    item.setRequestCount(rows.stream().mapToLong(ApiMetricsRaw::getReqCount).sum());
                                    item.setErrorRate(item.getRequestCount() > 0L ? rows.stream().mapToLong(ApiMetricsRaw::getErrorCount).sum() * 1.0D / item.getRequestCount() : 0D);
                                    long totalRequestCost = rows.stream().map(ApiMetricsRaw::getDelay).map(ApiMetricsDelayUtil::sum).mapToLong(Long::longValue).sum();
                                    final List<Map<Long, Integer>> merge = mergeDelay(rows);
                                    item.setP95(ApiMetricsDelayUtil.p95(merge, item.getRequestCount()));
                                    item.setP99(ApiMetricsDelayUtil.p99(merge, item.getRequestCount()));
                                    ApiMetricsDelayUtil.readMaxAndMin(merge, item::setMaxDelay, item::setMinDelay);
                                    item.setRequestCostAvg(item.getRequestCount() > 0L ? 1.0D * totalRequestCost / item.getRequestCount() : 0D);
                                    item.setQueryFrom(param.getStartAt());
                                    item.setQueryEnd(param.getEndAt());
                                    item.setGranularity(param.getGranularity());
                                    return item;
                                }
                        ))
        );
        List<ApiOfEachServer> apiOfEachServers = new ArrayList<>(collect.values());
        String orderBy = param.getOrderBy();
        if (StringUtils.isBlank(orderBy)) {
            orderBy = REQUEST_COUNT;
        }
        orderBy = orderBy.trim();
        Comparator<ApiOfEachServer> comparing = switch (orderBy) {
            case "requestCostAvg" -> (e1, e2) -> {
                Double p1 = Optional.ofNullable(e1.getRequestCostAvg()).orElse(0D);
                Double p2 = Optional.ofNullable(e2.getRequestCostAvg()).orElse(0D);
                return p1.compareTo(p2);
            };
            case ERROR_RATE -> (e1, e2) -> {
                Double p1 = Optional.ofNullable(e1.getErrorRate()).orElse(0D);
                Double p2 = Optional.ofNullable(e2.getErrorRate()).orElse(0D);
                return p1.compareTo(p2);
            };
            case "p95" -> (e1, e2) -> {
                Long p1 = Optional.ofNullable(e1.getP95()).orElse(0L);
                Long p2 = Optional.ofNullable(e2.getP95()).orElse(0L);
                return p1.compareTo(p2);
            };
            case "p99" -> (e1, e2) -> {
                Long p1 = Optional.ofNullable(e1.getP99()).orElse(0L);
                Long p2 = Optional.ofNullable(e2.getP99()).orElse(0L);
                return p1.compareTo(p2);
            };
            default -> (e1, e2) -> {
                Long p1 = Optional.ofNullable(e1.getRequestCount()).orElse(0L);
                Long p2 = Optional.ofNullable(e2.getRequestCount()).orElse(0L);
                return p1.compareTo(p2);
            };
        };
        apiOfEachServers.sort(comparing.reversed());
        return apiOfEachServers;
    }

    public ChartAndDelayOfApi delayOfApi(ApiChart param) {
        ChartAndDelayOfApi result = ChartAndDelayOfApi.create();
        Criteria criteria = ParticleSizeAnalyzer.of(result, param);
        List<ApiMetricsRaw> apiMetricsRaws = findRowByApiId(criteria, param.getApiId(), param);
        if (apiMetricsRaws.isEmpty()) {
            return result;
        }
        Map<Long, ChartAndDelayOfApi.Item> collect = apiMetricsRaws.stream().collect(
                Collectors.groupingBy(
                        ApiMetricsRaw::getTimeStart,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                rows -> {
                                    ApiMetricsRaw apiMetricsRaw = rows.get(0);
                                    long timeStart = apiMetricsRaw.getTimeStart();
                                    ChartAndDelayOfApi.Item item = new ChartAndDelayOfApi.Item();
                                    long totalBytes = rows.stream().map(ApiMetricsRaw::getBytes).map(ApiMetricsDelayUtil::fixDelayAsMap).mapToLong(ApiMetricsDelayUtil::sum).sum();
                                    long totalDelayMs = rows.stream().map(ApiMetricsRaw::getDelay).map(ApiMetricsDelayUtil::fixDelayAsMap).mapToLong(ApiMetricsDelayUtil::sum).sum();
                                    long reqCount = rows.stream().mapToLong(ApiMetricsRaw::getReqCount).sum();
                                    List<Map<Long, Integer>> merged = mergeDelay(rows);
                                    item.setTs(timeStart);
                                    item.setRps(totalDelayMs > 0L ? (totalBytes * 1000.0D / totalDelayMs) : 0D);
                                    item.setP95(ApiMetricsDelayUtil.p95(merged, reqCount));
                                    item.setP99(ApiMetricsDelayUtil.p99(merged, reqCount));
                                    ApiMetricsDelayUtil.readMaxAndMin(merged, item::setMaxDelay, item::setMinDelay);
                                    item.setRequestCostAvg(reqCount > 0L ? (1.0D * totalDelayMs / reqCount) : 0D);
                                    return item;
                                }
                        )
                )
        );
        //fix time and sort by time
        ChartSortUtil.fixAndSort(collect,
                param.getStartAt(), param.getEndAt(), param.getGranularity(),
                ChartAndDelayOfApi.Item::create,
                item -> {
                    result.getTs().add(item.getTs());
                    result.getRps().add(item.getRps());
                    result.getP95().add(item.getP95());
                    result.getP99().add(item.getP99());
                    result.getMaxDelay().add(item.getMaxDelay());
                    result.getMinDelay().add(item.getMinDelay());
                    result.getRequestCostAvg().add(item.getRequestCostAvg());
                });
        return result;
    }


    protected List<Map<Long, Integer>> mergeDelay(List<ApiMetricsRaw> rows) {
        final List<Map<Long, Integer>>[] delays = new List[rows.size()];
        for (int i = 0; i < rows.size(); i++) {
            final ApiMetricsRaw info = rows.get(i);
            if (null == info) {
                continue;
            }
            delays[i] = ApiMetricsDelayUtil.fixDelayAsMap(info.getDelay());
        }
        return ApiMetricsDelayUtil.merge(delays);
    }
}
