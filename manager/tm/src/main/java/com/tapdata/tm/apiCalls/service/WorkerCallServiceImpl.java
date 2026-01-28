package com.tapdata.tm.apiCalls.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Sorts;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.entity.WorkerCallStats;
import com.tapdata.tm.apiCalls.vo.ApiCountMetricVo;
import com.tapdata.tm.apiServer.entity.WorkerCallEntity;
import com.tapdata.tm.apiServer.enums.TimeGranularityType;
import com.tapdata.tm.apiServer.service.WorkerCallService;
import com.tapdata.tm.apiServer.service.compress.Compress;
import com.tapdata.tm.apiServer.service.metric.Metric;
import com.tapdata.tm.apiServer.vo.ApiCallMetricVo;
import com.tapdata.tm.apiServer.vo.metric.MetricDataBase;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.modules.entity.ModulesEntity;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.dto.ApiServerStatus;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.ApiWorkerServer;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/2 16:15 Create
 * @description
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class WorkerCallServiceImpl implements WorkerCallService {
    private WorkerService workerService;
    MongoTemplate mongoOperations;
    private ApiWorkerServer apiWorkerServer;
    MongoTemplate mongoTemplate;

    public ApiCallMetricVo find(String processId, Long from, Long to, Integer type, Integer granularity) {
        if (StringUtils.isBlank(processId)) {
            throw new BizException("api.call.metric.process.id.required");
        }
        granularity = Optional.ofNullable(granularity).orElse(0);
        Compress compress = Compress.call(granularity);
        to = Optional.ofNullable(to).orElse(System.currentTimeMillis());
        from = Optional.ofNullable(from).orElse(compress.defaultFrom(compress.fixTme(to)));
        compress.checkTimeRange(from, to);
        type = Optional.ofNullable(type).orElse(0);
        Metric<? extends ApiCallMetricVo.MetricBase> metric = Metric.call(type);
        Criteria criteria = Criteria.where(Tag.TIME_GRANULARITY).is(1)
                .and(Tag.DELETE).ne(true)
                .andOperator(Criteria.where(Tag.TIME_START).gte(from), Criteria.where(Tag.TIME_START).lte(to));
        criteria.and(Tag.PROCESS_ID).is(processId);
        Worker serverInfo = apiWorkerServer.getServerInfo(processId);
        Map<String, String> workerMap = apiWorkerServer.workerMap(serverInfo);
        Set<String> workerOIds = workerMap.keySet();

        ApiCallMetricVo vo = new ApiCallMetricVo();
        vo.setEndAs(to);
        vo.setStartAs(from);
        ApiCallMetricVo.ProcessMetric processMetricInit = new ApiCallMetricVo.ProcessMetric();
        processMetricInit.setProcessId(processId);
        processMetricInit.setProcessMetric(metric.mockMetric());
        vo.setProcessMetric(processMetricInit);
        vo.setWorkerMetrics(new ArrayList<>());
        workerMap.forEach((oid, name) -> {
            ApiCallMetricVo.WorkerMetrics workerMetric = new ApiCallMetricVo.WorkerMetrics();
            workerMetric.setWorkerMetric(metric.mockMetric());
            workerMetric.setWorkOid(oid);
            workerMetric.setWorkerName(name);
            vo.getWorkerMetrics().add(workerMetric);
        });
        try {
            if (workerOIds.isEmpty()) {
                return vo;
            }
            criteria.and(Tag.WORK_OID).in(workerOIds);
            Query query = Query.query(criteria);
            metric.fields(query);
            List<WorkerCallEntity> items = mongoOperations.find(query, WorkerCallEntity.class, "ApiCallInWorker");
            if (items.isEmpty()) {
                return vo;
            }
            Map<String, List<WorkerCallEntity>> groupByWorker = items.stream().collect(Collectors.groupingBy(WorkerCallEntity::getWorkOid));
            List<MetricDataBase> compressResult = compress.compress(items, metric::mergeTo, metric::mock);
            ApiCallMetricVo.MetricBase processMetric = metric.toResult(compressResult);
            ApiCallMetricVo.ProcessMetric processMetricInfo = new ApiCallMetricVo.ProcessMetric();
            processMetricInfo.setProcessId(processId);
            processMetricInfo.setProcessMetric(processMetric);
            vo.setProcessMetric(processMetricInfo);
            vo.setWorkerMetrics(new ArrayList<>());
            groupByWorker.forEach((workerOid, infos) -> {
                ApiCallMetricVo.MetricBase workerMetricInfo = metric.toResult(compressResult);
                ApiCallMetricVo.WorkerMetrics workerMetric = new ApiCallMetricVo.WorkerMetrics();
                workerMetric.setWorkOid(workerOid);
                workerMetric.setWorkerName(workerMap.get(workerOid));
                workerMetric.setWorkerMetric(workerMetricInfo);
                vo.getWorkerMetrics().add(workerMetric);
            });
            return vo;
        } finally {
            compress.fixTime(vo, metric::mock, compress.fixTme(from), compress.plus(compress.fixTme(to)));
        }
    }

    public ApiCountMetricVo findWorkerApiCalls(String processId) {
        Worker server = apiWorkerServer.getServerInfo(processId);
        List<WorkerCallStats> apiCallInWorker = apiCallInWorkers(processId);
        ApiCountMetricVo vo = new ApiCountMetricVo();
        ApiCountMetricVo.ProcessMetric processMetric = new ApiCountMetricVo.ProcessMetric();
        processMetric.setProcessId(processId);
        processMetric.setServerName(server.getHostname());
        vo.setProcessMetric(processMetric);
        if (apiCallInWorker.isEmpty()) {
            return vo;
        }
        Map<String, String> workerMap = apiWorkerServer.workerMap(server);
        Map<String, String> apiMap = apiMap();
        Map<String, ApiCountMetricVo.ApiItem> processApiCountMap = new HashMap<>(16);
        Map<String, Map<String, ApiCountMetricVo.ApiItem>> workerApiCountMap = new HashMap<>(16);
        apiCallInWorker.stream()
                .filter(Objects::nonNull)
                .filter(e -> StringUtils.isNotBlank(e.getAllPathId()))
                .collect(Collectors.groupingBy(WorkerCallStats::getAllPathId))
                .forEach((apiId, list) -> {
                    String apiName = apiMap.get(apiId);
                    list.forEach(w -> collectApiCall(w, apiName, workerApiCountMap, processApiCountMap));
                });
        workerApiCountMap.forEach((workOid, apiCountMap) -> {
            ApiCountMetricVo.WorkerMetrics workerMetric = new ApiCountMetricVo.WorkerMetrics();
            workerMetric.setWorkOid(workOid);
            workerMetric.setWorkerName(workerMap.get(workOid));
            workerMetric.setWorkerMetric(new ArrayList<>(apiCountMap.values()));
            vo.getWorkerMetrics().add(workerMetric);
        });
        processMetric.setProcessMetric(new ArrayList<>(processApiCountMap.values()));
        processMetric.getProcessMetric().sort(Comparator.comparing(ApiCountMetricVo.ApiItem::getCount).reversed());
        vo.getWorkerMetrics().forEach(w -> w.getWorkerMetric().sort(Comparator.comparing(ApiCountMetricVo.ApiItem::getCount).reversed()));
        return vo;
    }

    Map<String, String> apiMap() {
        Query query = Query.query(new Criteria());
        query.fields().include("name", "_id");
        List<ModulesEntity> modules = mongoOperations.find(query, ModulesEntity.class, "Modules");
        if (modules.isEmpty()) {
            return new HashMap<>(0);
        }
        return modules.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(e -> e.getId().toHexString(), e -> null == e.getName() ? "-" : e.getName(), (m1, m2) -> m2));
    }

    void collectApiCall(WorkerCallStats apiCallStats,
                        String apiName,
                        Map<String, Map<String, ApiCountMetricVo.ApiItem>> workerApiCountMap,
                        Map<String, ApiCountMetricVo.ApiItem> processApiCountMap) {
        collectApiCallOnce(apiCallStats, apiName, processApiCountMap);
        String workOid = apiCallStats.getWorkOid();
        Map<String, ApiCountMetricVo.ApiItem> apiItemMap = workerApiCountMap.computeIfAbsent(workOid, key -> new HashMap<>());
        collectApiCallOnce(apiCallStats, apiName, apiItemMap);
    }

    void collectApiCallOnce(WorkerCallStats apiCallStats,
                            String apiName,
                            Map<String, ApiCountMetricVo.ApiItem> apiCountMap) {
        ApiCountMetricVo.ApiItem ofProcess = apiCountMap.computeIfAbsent(apiCallStats.getAllPathId(), k -> {
            ApiCountMetricVo.ApiItem apiItem = new ApiCountMetricVo.ApiItem();
            apiItem.setApiId(apiCallStats.getAllPathId());
            apiItem.setApiName(apiName);
            return apiItem;
        });
        ofProcess.setCount(ofProcess.getCount() + apiCallStats.getTotalCount());
        ofProcess.setErrorCount(ofProcess.getErrorCount() + apiCallStats.getNotOkCount());
    }

    List<WorkerCallStats> apiCallInWorkers(String processId) {
        return mongoOperations.find(
                Query.query(Criteria.where(Tag.PROCESS_ID).is(processId).and(Tag.DELETE).ne(true)),
                WorkerCallStats.class, "WorkerCallStats");
    }


    public void collectApiCallCountGroupByWorker(String processId) {
        List<WorkerCallStats> apiCallInWorker = apiCallInWorkers(processId);
        Map<String, WorkerCallStats> collect = apiCallInWorker.stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(
                        e -> String.format("%s_%s_%s", e.getProcessId(), e.getWorkOid(), e.getAllPathId()),
                        e -> e, (e1, e2) -> e2));
        String lastApiCallId = collect.isEmpty() ? null : collect.values().stream().map(WorkerCallStats::getLastCallId).findFirst().orElse(null);
        Criteria criteria = Criteria.where(Tag.WORK_OID).ne(null)
                .and(Tag.ALL_PATH_ID).ne(null)
                .and("api_gateway_uuid").is(processId);
        List<Criteria> idCriteria = new ArrayList<>();
        if (null != lastApiCallId) {
            idCriteria.add(Criteria.where("_id").gt(new ObjectId(lastApiCallId)));
        }
        Query query = Query.query(criteria).limit(1).with(Sort.by(Sort.Order.desc("_id")));
        ApiCallEntity topOne = mongoOperations.findOne(query, ApiCallEntity.class);
        if (null == topOne) {
            return;
        }
        idCriteria.add(Criteria.where("_id").lte(topOne.getId()));
        criteria.andOperator(idCriteria);
        criteria.and("supplement").ne(true);
        Query callQuery = Query.query(criteria);
        callQuery.fields().include(Tag.ALL_PATH_ID, Tag.WORK_OID, "codeMsg", "httpStatus", "code", "succeed");
        List<ApiCallEntity> apiCalls = mongoOperations.find(callQuery, ApiCallEntity.class, MongoUtils.getCollectionName(ApiCallEntity.class));
        Map<String, Map<String, WorkerCallStats>> groupByApiAndWorker = groupCallResult(processId, apiCalls);
        List<WorkerCallStats> mappedResults = new ArrayList<>();
        groupByApiAndWorker.values().forEach(apiMap -> mappedResults.addAll(apiMap.values()));
        if (mappedResults.isEmpty()) {
            return;
        }
        Function<WorkerCallStats, Query> queryBuilder = entity -> {
            Criteria criteriaBulk = Criteria.where(Tag.ALL_PATH_ID).is(entity.getAllPathId())
                    .and(Tag.PROCESS_ID).is(entity.getProcessId())
                    .and(Tag.DELETE).ne(true)
                    .and(Tag.WORK_OID).is(entity.getWorkOid());
            return Query.query(criteriaBulk);
        };
        Function<WorkerCallStats, Update> updateBuilder = entity -> {
            Update update = new Update();
            update.set("totalCount", entity.getTotalCount());
            update.set("notOkCount", entity.getNotOkCount());
            update.set("lastCallId", entity.getLastCallId());
            update.currentDate("updatedAt");
            return update;
        };
        try {
            BulkOperations bulkOps = mongoOperations.bulkOps(BulkOperations.BulkMode.ORDERED, WorkerCallStats.class);
            for (WorkerCallStats e : mappedResults) {
                WorkerCallStats oldStats = collect.get(String.format("%s_%s_%s", e.getProcessId(), e.getWorkOid(), e.getAllPathId()));
                e.setTotalCount(e.getTotalCount() + Optional.ofNullable(oldStats).map(WorkerCallStats::getTotalCount).orElse(0L));
                e.setNotOkCount(e.getNotOkCount() + Optional.ofNullable(oldStats).map(WorkerCallStats::getNotOkCount).orElse(0L));
                e.setLastCallId(topOne.getId().toHexString());
                Query queryTemp = queryBuilder.apply(e);
                Update update = updateBuilder.apply(e);
                bulkOps.upsert(queryTemp, update);
            }
            bulkOps.execute();
        } catch (Exception e) {
            log.error("bulkUpsert WorkerCallEntity error", e);
        }
    }

    protected Map<String, Map<String, WorkerCallStats>> groupCallResult(String processId, List<ApiCallEntity> apiCalls) {
        Map<String, Map<String, WorkerCallStats>> groupByApiAndWorker = new HashMap<>();
        for (ApiCallEntity apiCall : apiCalls) {
            if (null == apiCall) {
                continue;
            }
            //兼容旧数据
            if ("/openapi-readOnly.json".equals(apiCall.getReq_path())) {
                continue;
            }
            Map<String, WorkerCallStats> map = groupByApiAndWorker.computeIfAbsent(apiCall.getWorkOid(), k -> new HashMap<>());
            WorkerCallStats item = map.computeIfAbsent(apiCall.getAllPathId(), k -> {
                WorkerCallStats workerCallStats = new WorkerCallStats();
                workerCallStats.setAllPathId(apiCall.getAllPathId());
                workerCallStats.setWorkOid(apiCall.getWorkOid());
                workerCallStats.setProcessId(processId);
                workerCallStats.setTotalCount(0);
                workerCallStats.setNotOkCount(0);
                return workerCallStats;
            });
            item.setTotalCount(1 + item.getTotalCount());
            boolean isOk = apiCall.isSucceed();
            item.setNotOkCount((isOk ? 0 : 1) + item.getNotOkCount());
        }
        return groupByApiAndWorker;
    }

    /**
     * 统计：
     * 获取这种运行的worker列表
     * 获取每个worker各自最近的统计时间
     * 按worker&时间分组（min）查询ApiCalls,
     */
    public void metric() {
        final Criteria serverCriteria = Criteria.where("worker_type").is("api-server");
        final Query serverQuery = Query.query(serverCriteria);
        final List<WorkerDto> apiServers = workerService.findAll(serverQuery);
        apiServers.forEach(server -> {
            if (null == server) {
                return;
            }
            Optional.ofNullable(server.getWorkerStatus())
                    .map(ApiServerStatus::getWorkers)
                    .map(Map::values)
                    .orElse(new ArrayList<>())
                    .forEach(info -> {
                        if (info.getOid() != null) {
                            metricWorker(info.getOid());
                        }
                    });
        });
    }

    void closeFeatures(List<CompletableFuture<Void>> futures) {
        if (futures.isEmpty()) {
            return;
        }
        futures.forEach(future -> {
            try {
                future.get();
            } catch (ExecutionException e) {
                log.error("metricWorker error", e);
            } catch (InterruptedException e) {
                log.error("metricWorker interrupted", e);
                Thread.currentThread().interrupt();
            }
        });
    }

    public void bulkUpsert(List<WorkerCallEntity> entities) {
        bulkUpsert(entities, this::buildDefaultQuery, this::buildDefaultUpdate);
    }


    public void bulkUpsert(List<WorkerCallEntity> entities,
                           Function<WorkerCallEntity, Query> queryBuilder,
                           Function<WorkerCallEntity, Update> updateBuilder) {
        try {
            if (entities == null || entities.isEmpty()) {
                return;
            }
            BulkOperations bulkOps = mongoOperations.bulkOps(BulkOperations.BulkMode.ORDERED, WorkerCallEntity.class);
            for (WorkerCallEntity entity : entities) {
                entity.setTtlKey(new Date(entity.getTimeStart()));
                Query query = queryBuilder.apply(entity);
                Update update = updateBuilder.apply(entity);
                bulkOps.upsert(query, update);
            }
            bulkOps.execute();
        } catch (Exception e) {
            log.error("bulkUpsert WorkerCallEntity error", e);
        }
    }

    private Query buildDefaultQuery(WorkerCallEntity entity) {
        Criteria criteria = Criteria.where(Tag.TIME_START).is(entity.getTimeStart())
                .and(Tag.DELETE).is(entity.getDelete())
                .and(Tag.TIME_GRANULARITY).is(entity.getTimeGranularity())
                .and(Tag.PROCESS_ID).is(entity.getProcessId())
                .and(Tag.API_ID).is(entity.getApiId())
                .and(Tag.WORK_OID).is(entity.getWorkOid());
        return Query.query(criteria);
    }

    private Update buildDefaultUpdate(WorkerCallEntity entity) {
        Update update = new Update();
        update.set("reqCount", entity.getReqCount());
        update.set("rps", entity.getRps());
        update.set("delays", entity.getDelays());
        update.set("p50", entity.getP50());
        update.set("p95", entity.getP95());
        update.set("p99", entity.getP99());
        update.set("errorCount", entity.getErrorCount());
        update.set("errorRate", entity.getErrorRate());
        update.currentDate("updatedAt");
        update.set("lastApiCallId", entity.getLastApiCallId());
        update.set("ttlKey", entity.getTtlKey());
        return update;
    }

    void metricWorker(String workerOid) {
        Criteria criteria = Criteria.where(Tag.WORK_OID).is(workerOid)
                .and(Tag.TIME_GRANULARITY).is(TimeGranularityType.MINUTE.getCode())
                .and(Tag.DELETE).is(false);
        Query query = Query.query(criteria);
        query.limit(1);
        query.with(Sort.by(Sort.Order.desc("lastApiCallId")));
        WorkerCallEntity lastOne = mongoOperations.findOne(query, WorkerCallEntity.class);
        Long queryFrom = null;
        if (null != lastOne) {
            queryFrom = lastOne.getTimeStart();
        }
        final WorkerCallsInfoGenerator.Acceptor acceptor = this::bulkUpsert;
        Criteria criteriaCall = Criteria.where(Tag.WORK_OID).is(workerOid);
        List<Criteria> timeCriteria = new ArrayList<>();
        Optional.ofNullable(queryFrom).ifPresent(time -> timeCriteria.add(Criteria.where(Tag.REQ_TIME).gte(time)));
        timeCriteria.add(Criteria.where(Tag.REQ_TIME).lt(System.currentTimeMillis()));
        criteriaCall.andOperator(timeCriteria);
        final MongoCollection<Document> collection = mongoTemplate.getCollection("ApiCall");
        final Query queryCall = Query.query(criteriaCall);
        queryCall.fields().include("_id", "allPathId", "api_gateway_uuid", "latency", "req_bytes", "reqTime", "code", "httpStatus", "createTime", "dataQueryTotalTime", "workOid", "req_path", "succeed");
        final Document queryObject = queryCall.getQueryObject();
        final FindIterable<Document> iterable =
                collection.find(queryObject, Document.class)
                        .projection(queryCall.getFieldsObject())
                        .sort(Sorts.ascending(Tag.REQ_TIME))
                        .batchSize(1000);
        try (final MongoCursor<Document> cursor = iterable.iterator();
             WorkerCallsInfoGenerator generator = new WorkerCallsInfoGenerator(acceptor)) {
            while (cursor.hasNext()) {
                final Document entity = cursor.next();
                generator.append(entity);
            }
        }
    }

    @Override
    public List<WorkerCallEntity> findData(Query query) {
        return mongoOperations.find(query, WorkerCallEntity.class);
    }

    public static class Tag {
        private Tag() {
        }

        public static final String WORK_OID = "workOid";
        public static final String TIME_START = "timeStart";
        public static final String TIME_GRANULARITY = "timeGranularity";
        public static final String DELETE = "delete";
        public static final String PROCESS_ID = "processId";


        public static final String REQ_TIME = "reqTime";
        public static final String RES_TIME = "resTime";
        public static final String ALL_PATH_ID = "allPathId";
        public static final String API_ID = "apiId";
    }
}
