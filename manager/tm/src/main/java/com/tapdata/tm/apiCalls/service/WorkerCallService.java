package com.tapdata.tm.apiCalls.service;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.internal.bulk.WriteRequest;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.entity.WorkerCallEntity;
import com.tapdata.tm.apiCalls.service.compress.Compress;
import com.tapdata.tm.apiCalls.service.metric.Metric;
import com.tapdata.tm.apiCalls.vo.ApiCallMetricVo;
import com.tapdata.tm.apiCalls.vo.ApiCallStats;
import com.tapdata.tm.apiCalls.vo.ApiCountMetricVo;
import com.tapdata.tm.apiCalls.vo.WorkerCallsInfo;
import com.tapdata.tm.apiCalls.vo.metric.MetricDataBase;
import com.tapdata.tm.apicallminutestats.dto.ApiCallMinuteStatsDto;
import com.tapdata.tm.apicallminutestats.service.ApiCallMinuteStatsService;
import com.tapdata.tm.apicallstats.dto.ApiCallStatsDto;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.utils.EntityUtils;
import com.tapdata.tm.worker.dto.WorkerDto;
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
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.ConditionalOperators;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.SortOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.tapdata.tm.utils.DocumentUtils.getLong;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/9/2 16:15 Create
 * @description
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class WorkerCallService {
    private WorkerService workerService;
    MongoTemplate mongoOperations;
    ApiCallMinuteStatsService apiCallMinuteStatsService;

    public ApiCallMetricVo find(String processId, Long from, Long to, Integer type, Integer granularity) {
        if (StringUtils.isBlank(processId)) {
            throw new BizException("api.call.metric.process.id.required");
        }
        from = Optional.ofNullable(from).orElse(System.currentTimeMillis() - 5 * 60 * 1000L);
        to = Optional.ofNullable(to).orElse(System.currentTimeMillis());
        type = Optional.ofNullable(type).orElse(0);
        Metric<? extends ApiCallMetricVo.MetricBase> metric = Metric.call(type);
        Criteria criteria = Criteria.where(Tag.TIME_START).gte(from)
                .and(Tag.TIME_START).lte(to)
                .and(Tag.TIME_GRANULARITY).is(0)
                .and(Tag.DELETE).ne(true);
        if (StringUtils.isNotBlank(processId)) {
            criteria.and(Tag.PROCESS_ID).is(processId);
        }
        ApiCallMetricVo vo = new ApiCallMetricVo();
        Query query = Query.query(criteria);
        metric.fields(query);
        List<WorkerCallEntity> items = mongoOperations.find(query, WorkerCallEntity.class, "ApiCallInWorker");
        Map<String, List<WorkerCallEntity>> groupByWorker = items.stream().collect(Collectors.groupingBy(WorkerCallEntity::getWorkOid));
        granularity = Optional.ofNullable(granularity).orElse(0);
        Compress compress = Compress.call(granularity);

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
            workerMetric.setWorkerName(workerOid);
            workerMetric.setWorkerMetric(workerMetricInfo);
            vo.getWorkerMetrics().add(workerMetric);
        });
        return vo;
    }


    public Page<ApiCountMetricVo.ProcessMetric> findApiCallsOfServer(String processId, Integer page, Integer size) {
        Criteria criteria = Criteria.where(Tag.WORK_OID).ne(null)
                .and("allPathId").ne(null);
        if (StringUtils.isNotBlank(processId)) {
            criteria.and(Tag.PROCESS_ID).is(processId);
        }
        MatchOperation matchStage = Aggregation.match(criteria);

        // 2. 分组阶段 - 按apiId和workOid分组
        GroupOperation groupStage = Aggregation.group("apiId", Tag.PROCESS_ID, Tag.WORK_OID)
                .count().as("totalCount")
                .sum(ConditionalOperators.when(Criteria.where("codeMsg").is("ok")).then(1).otherwise(0)).as("okCount")
                .sum(ConditionalOperators.when(Criteria.where("codeMsg").ne("ok")).then(1).otherwise(0)).as("notOkCount");

        // 3. 排序阶段 - 按apiId和workOid排序
        SortOperation sortStage = Aggregation.sort(
                Sort.Direction.ASC, "apiId", Tag.PROCESS_ID, Tag.WORK_OID
        );

        // 构建聚合管道
        Aggregation aggregation = Aggregation.newAggregation(
                matchStage,
                groupStage,
                sortStage
        );

        AggregationResults<ApiCallStats> results = mongoOperations.aggregate(
                aggregation, "ApiCall", ApiCallStats.class
        );

        List<ApiCallStats> mappedResults = results.getMappedResults();

        return null;
    }

    public ApiCountMetricVo.WorkerMetrics findApiCallsOrWorker(String processId) {

        return null;
    }


    public List<ApiCallStatsDto> aggregateWithFacet(String lastApiCallId, String rightNow) {
        String apiCallCollectionName;
        try {
            apiCallCollectionName = EntityUtils.documentAnnotationValue(ApiCallEntity.class);
        } catch (Exception e) {
            throw new BizException("Get ApiCallEntity's collection name failed", e);
        }

        MongoCollection<Document> apiCallCollection = mongoOperations.getCollection(apiCallCollectionName);

        // 构建匹配条件
        Document match = new Document();
        if (StringUtils.isNotBlank(lastApiCallId)) {
            match.append("_id", new Document("$gt", new ObjectId(lastApiCallId)));
        }
        match.append("_id", new Document("$lte", new ObjectId(rightNow)));
        match.append("allPathId", new Document("$ne", null))
                .append(Tag.WORK_OID, new Document("$ne", null))
                .append("api_gateway_uuid", new Document("$ne", null));

        // 构建facet统计
        Document facet = new Document();

        // 按api_gateway_uuid和workOid分组统计
        List<Document> byApiGatewayAndWorkOid = Arrays.asList(
                new Document("$match", match),
                new Document("$group",
                        new Document("_id",
                                new Document("apiGatewayUuid", "$api_gateway_uuid")
                                        .append("workOid", "$" + Tag.WORK_OID)
                                        .append("allPathId", "$allPathId")
                        )
                                .append("callTotalCount", new Document("$sum", 1L))
                                .append("transferDataTotalBytes", new Document("$sum", "$req_bytes"))
                                .append("callAlarmTotalCount", new Document("$sum",
                                        new Document("$cond", Arrays.asList(
                                                new Document("$ne", Arrays.asList("$code", "200")),
                                                1L,
                                                0L
                                        ))
                                ))
                                .append("responseDataRowTotalCount", new Document("$sum", "$res_rows"))
                                .append("totalResponseTime", new Document("$sum", "$latency"))
                                .append("maxResponseTime", new Document("$max", "$latency"))
                                .append("lastApiCallId", new Document("$max", "$_id"))
                                .append("clientIds", new Document("$addToSet", "$user_info.clientId"))
                )
        );

        facet.append("byApiGatewayAndWorkOid", byApiGatewayAndWorkOid);

        List<Document> pipeline = Arrays.asList(
                new Document("$match", match),
                new Document("$facet", facet)
        );

        // 执行查询并处理结果
        List<ApiCallStatsDto> result = new ArrayList<>();

        try (MongoCursor<Document> iterator = apiCallCollection.aggregate(pipeline, Document.class)
                .allowDiskUse(true).iterator()) {

            if (iterator.hasNext()) {
                Document doc = iterator.next();
                List<Document> statsDocs = doc.getList("byApiGatewayAndWorkOid", Document.class);

                if (statsDocs != null) {
                    List<ApiCallStatsDto> dtos = statsDocs.stream()
                            .map(this::convertDocumentToDto)
                            .map(e -> {
                                e.setLastApiCallId(rightNow);
                                return e;
                            }).toList();

                    // 按apiGatewayUuid分组
                    result.addAll(dtos);
                }
            }
        }

        return result;
    }

    private ApiCallStatsDto convertDocumentToDto(Document doc) {
        ApiCallStatsDto dto = new ApiCallStatsDto();

        // 解析分组ID
        Document idDoc = doc.get("_id", Document.class);
        if (idDoc != null) {
            dto.setProcessId(idDoc.getString("apiGatewayUuid"));
            dto.setWorkerOid(idDoc.getString("workOid"));
            dto.setModuleId(idDoc.getString("allPathId"));
        }

        // 设置统计字段
        dto.setCallTotalCount(getLong(doc, "callTotalCount"));
        dto.setTransferDataTotalBytes(getLong(doc, "transferDataTotalBytes"));
        dto.setCallAlarmTotalCount(getLong(doc, "callAlarmTotalCount"));
        dto.setResponseDataRowTotalCount(getLong(doc, "responseDataRowTotalCount"));
        dto.setTotalResponseTime(getLong(doc, "totalResponseTime"));
        dto.setMaxResponseTime(getLong(doc, "responseDataRowTotalCount"));

        // 处理lastApiCallId
        ObjectId lastApiCallId = doc.getObjectId("lastApiCallId");
        if (lastApiCallId != null) {
            dto.setLastApiCallId(lastApiCallId.toString());
        }

        // 处理clientIds
        List<String> clientIds = doc.getList("clientIds", String.class);
        if (clientIds != null) {
            dto.getClientIds().addAll(clientIds.stream()
                    .filter(Objects::nonNull)
                    .toList());
        }

        return dto;
    }


    public List<ApiCallMinuteStatsDto> aggregateMinuteByAllPathId(String allPathId, String lastApiCallId, Date startTime) {
        List<ApiCallMinuteStatsDto> apiCallMinuteStatsDtoList = new ArrayList<>();
        String apiCallCollectionName;
        try {
            apiCallCollectionName = EntityUtils.documentAnnotationValue(ApiCallEntity.class);
        } catch (Exception e) {
            throw new BizException("Get ApiCallEntity's collection name failed", e);
        }
        MongoCollection<Document> apiCallCollection = mongoOperations.getCollection(apiCallCollectionName);

        // Build aggregation pipeline
        Document match = new Document("allPathId", allPathId);
        if (StringUtils.isNotBlank(lastApiCallId)) {
            match.append("_id", new Document("$gt", new ObjectId(lastApiCallId)));
        }
        if (null != startTime) {
            match.append("createTime", new Document("$gte", startTime));
        }
        List<Document> pipeline = new ArrayList<>();
        pipeline.add(new Document("$match", match));
        pipeline.add(new Document("$project", new Document("year", new Document("$year", "$createTime"))
                .append("month", new Document("$month", "$createTime"))
                .append("day", new Document("$dayOfMonth", "$createTime"))
                .append("hour", new Document("$hour", "$createTime"))
                .append("minute", new Document("$minute", "$createTime"))
                .append("res_rows", 1)
                .append("latency", 1)
                .append("req_bytes", 1)
        ));
        Document group = new Document("_id", groupByMinute())
                .append("responseDataRowTotalCount", new Document("$sum", "$res_rows"))
                .append("totalResponseTime", new Document("$sum", "$latency"))
                .append("transferDataTotalBytes", new Document("$sum", "$req_bytes"))
                .append("lastApiCallId", new Document("$last", "$_id"));
        pipeline.add(new Document("$group", group));
        if (log.isDebugEnabled()) {
            StringBuilder pipelineString = new StringBuilder();
            pipeline.forEach(document -> pipelineString.append(document.toJson()).append(System.lineSeparator()));
            log.debug("ApiCallStatsService.aggregateMinuteByAllPathId pipeline: {}{}", System.lineSeparator(), pipelineString);
        }
        // Execute aggregation
        try (
                MongoCursor<Document> iterator = apiCallCollection.aggregate(pipeline, Document.class).allowDiskUse(true).iterator()
        ) {
            while (iterator.hasNext()) {
                Document document = iterator.next();
                ApiCallMinuteStatsDto apiCallMinuteStatsDto = new ApiCallMinuteStatsDto();
                apiCallMinuteStatsDto.setModuleId(allPathId);

                apiCallMinuteStatsDto.setResponseDataRowTotalCount(getLong(document, "responseDataRowTotalCount"));
                apiCallMinuteStatsDto.setTotalResponseTime(getLong(document, "totalResponseTime"));
                apiCallMinuteStatsDto.setTransferDataTotalBytes(getLong(document, "transferDataTotalBytes"));
                // responseTimePerRow, rowPerSecond
                apiCallMinuteStatsService.calculate(apiCallMinuteStatsDto);
                if (null != document.get("lastApiCallId")) {
                    apiCallMinuteStatsDto.setLastApiCallId(document.getObjectId("lastApiCallId").toString());
                }
                // apiCallTime: year, month, day, hour, minute
                Document id = document.get("_id", Document.class);
                Instant apiCallTime = LocalDateTime.of(
                        id.getInteger("year"),
                        id.getInteger("month"),
                        id.getInteger("day"),
                        id.getInteger("hour"),
                        id.getInteger("minute")
                ).toInstant(ZoneOffset.UTC);
                apiCallMinuteStatsDto.setApiCallTime(Date.from(apiCallTime));

                apiCallMinuteStatsDtoList.add(apiCallMinuteStatsDto);
            }
        }

        return apiCallMinuteStatsDtoList;
    }

    public Document groupByMinute() {
        return new Document("year", "$year")
                .append("month", "$month")
                .append("day", "$day")
                .append("hour", "$hour")
                .append("minute", "$minute");
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
        final CompletableFuture<Void> supplyAsync = CompletableFuture.runAsync(() -> {
        });
        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        try {
            apiServers.forEach(server -> {
                if (server.getWorker_status() instanceof Map<?, ?> status) {
                    if (status.get("workers") instanceof Map<?, ?> workers) {
                        workers.forEach((k, v) -> {
                            if (v instanceof Map<?, ?> info && info.get("oid") instanceof String oid) {
                                futures.add(supplyAsync.thenRunAsync(() -> metricWorker(oid)));
                            }
                        });
                    }
                }
            });
        } finally {
            if (!futures.isEmpty()) {
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
        }
    }

    public BulkWriteResult bulkUpsert(List<WorkerCallEntity> entities) {
        return bulkUpsert(entities, this::buildDefaultQuery, this::buildDefaultUpdate);
    }


    public BulkWriteResult bulkUpsert(List<WorkerCallEntity> entities,
                                      Function<WorkerCallEntity, Query> queryBuilder,
                                      Function<WorkerCallEntity, Update> updateBuilder) {
        try {
            if (entities == null || entities.isEmpty()) {
                return BulkWriteResult.acknowledged(WriteRequest.Type.INSERT, 0, 0, null, null);
            }
            BulkOperations bulkOps = mongoOperations.bulkOps(BulkOperations.BulkMode.ORDERED, WorkerCallEntity.class);
            for (WorkerCallEntity entity : entities) {
                Query query = queryBuilder.apply(entity);
                Update update = updateBuilder.apply(entity);
                bulkOps.upsert(query, update);
            }
            return bulkOps.execute();
        } catch (Exception e) {
            log.error("bulkUpsert WorkerCallEntity error", e);
            return BulkWriteResult.acknowledged(WriteRequest.Type.INSERT, 0, 0, null, null);
        }
    }

    private Query buildDefaultQuery(WorkerCallEntity entity) {
        Criteria criteria = Criteria.where(Tag.TIME_START).is(entity.getTimeStart())
                .and(Tag.DELETE).is(entity.getDelete())
                .and(Tag.TIME_GRANULARITY).is(entity.getTimeGranularity())
                .and(Tag.PROCESS_ID).is(entity.getProcessId())
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
        return update;
    }

    void metricWorker(String workerOid) {
        Criteria criteria = Criteria.where(Tag.WORK_OID).is(workerOid)
                .and(Tag.TIME_GRANULARITY).is(0)
                .and(Tag.DELETE).ne(true);
        Query query = Query.query(criteria);
        query.limit(1);
        query.with(Sort.by(Sort.Order.desc(Tag.TIME_START)));
        WorkerCallEntity lastOne = mongoOperations.findOne(query, WorkerCallEntity.class);
        Long queryFrom = null;
        Long queryTo = System.currentTimeMillis();
        if (null != lastOne) {
            queryFrom = lastOne.getTimeStart();
        }
        final WorkerCallsInfoGenerator.Acceptor acceptor = this::bulkUpsert;
        List<WorkerCallsInfo> calls = null;
        long skip = 0;
        long size = 1000;
        Criteria criteriaCall = Criteria.where(Tag.WORK_OID).is(workerOid);
        criteriaCall.and(Tag.RES_TIME).ne(null);
        Optional.ofNullable(queryFrom).ifPresent(time -> criteriaCall.and(Tag.REQ_TIME).gte(time));
        criteriaCall.and(Tag.REQ_TIME).lte(queryTo);
        try (WorkerCallsInfoGenerator generator = new WorkerCallsInfoGenerator(acceptor)) {
            do {
                Query queryCall = Query.query(criteriaCall);
                queryCall.skip(skip);
                calls = mongoOperations.find(queryCall, WorkerCallsInfo.class, "ApiCall");
                if (!calls.isEmpty()) {
                    generator.append(calls);
                    skip += size;
                }
            } while (!calls.isEmpty());
        }
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
    }
}
