package com.tapdata.tm.v2.api.monitor.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Sorts;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.service.WorkerCallServiceImpl;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import com.tapdata.tm.v2.api.monitor.main.enums.TimeGranularity;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/26 11:44 Create
 * @description
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class ApiMetricsRawScheduleExecutor {
    public static final String OBJECT_ID = "_id";
    MongoTemplate mongoTemplate;

    public MetricInstanceFactory create() {
        return new MetricInstanceFactory(this::saveApiMetricsRaw, this::findMetricStart);
    }

    public void aggregateApiCall() {
        final String collectionName = MongoUtils.getCollectionNameIgnore(ApiCallEntity.class);
        if (StringUtils.isBlank(collectionName)) {
            return;
        }
        ObjectId lastCallId = lastOne();
        try (MetricInstanceFactory acceptor = create()) {
            final MongoCollection<Document> collection = mongoTemplate.getCollection(collectionName);
            final Criteria criteria = Criteria.where("deleted").ne(true)
                    .and("supplement").ne(true);
            if (Objects.nonNull(lastCallId)) {
                criteria.and(OBJECT_ID).gt(lastCallId);
            }
            criteria.and("reqTime").lt(System.currentTimeMillis());
            final Query query = Query.query(criteria);
            query.fields().include(OBJECT_ID, "allPathId", "workerOid", "api_gateway_uuid", "latency", "req_bytes", "reqTime", "code", "httpStatus", "createTime", "dataQueryTotalTime", "workOid", "req_path");
            final Document queryObject = query.getQueryObject();
            final FindIterable<Document> iterable =
                    collection.find(queryObject, Document.class)
                            .sort(Sorts.ascending("reqTime"))
                            .batchSize(2000);
            try (final MongoCursor<Document> cursor = iterable.iterator()) {
                while (cursor.hasNext()) {
                    final Document entity = cursor.next();
                    acceptor.accept(entity);
                }
            }
        }
    }

    protected ObjectId lastOne() {
        List<Document> pipeline = Arrays.asList(
                new Document("$match",
                        new Document("timeGranularity", TimeGranularity.MINUTE.getType())
                                .append("metricType", MetricTypes.API_SERVER.getType())
                ),
                new Document("$group",
                        new Document("_id", "$apiId")
                                .append("maxLastCallId", new Document("$max", "$lastCallId"))
                ),
                new Document("$group",
                        new Document("_id", null)
                                .append("minOfMaxLastCallId", new Document("$min", "$maxLastCallId"))
                )
        );
        List<Document> results = mongoTemplate.getCollection("ApiMetricsRaw")
                .aggregate(pipeline)
                .into(new ArrayList<>());
        if (!results.isEmpty()) {
            Document resultDoc = results.get(0);
            Object minValue = resultDoc.get("minOfMaxLastCallId");
            if (minValue instanceof org.bson.types.ObjectId) {
                return (org.bson.types.ObjectId) minValue;
            } else {
                return null;
            }
        }
        return null;
    }

    void saveApiMetricsRaw(List<ApiMetricsRaw> apiMetricsRawList) {
        if (CollectionUtils.isEmpty(apiMetricsRawList)) {
            return;
        }
        bulkUpsert(apiMetricsRawList);
    }

    void bulkUpsert(List<ApiMetricsRaw> entities) {
        bulkUpsert(entities, this::buildDefaultQuery, this::buildDefaultUpdate);
    }


    void bulkUpsert(List<ApiMetricsRaw> entities,
                    Function<ApiMetricsRaw, Query> queryBuilder,
                    Function<ApiMetricsRaw, Update> updateBuilder) {
        try {
            BulkOperations bulkOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, ApiMetricsRaw.class);
            for (ApiMetricsRaw entity : entities) {
                entity.setTtlKey(new Date(entity.getTimeStart() * 1000L));
                Query query = queryBuilder.apply(entity);
                Update update = updateBuilder.apply(entity);
                bulkOps.upsert(query, update);
            }
            bulkOps.execute();
        } catch (Exception e) {
            log.error("bulkUpsert ApiMetricsRaw error", e);
        }
    }

    protected Query buildDefaultQuery(ApiMetricsRaw entity) {
        Criteria criteria = Criteria.where(WorkerCallServiceImpl.Tag.TIME_START).is(entity.getTimeStart())
                .and(WorkerCallServiceImpl.Tag.TIME_GRANULARITY).is(entity.getTimeGranularity())
                .and(WorkerCallServiceImpl.Tag.PROCESS_ID).is(entity.getProcessId())
                .and(WorkerCallServiceImpl.Tag.API_ID).is(entity.getApiId())
                .and("metricType").is(entity.getMetricType());
        return Query.query(criteria);
    }

    protected Update buildDefaultUpdate(ApiMetricsRaw entity) {
        Update update = new Update();
        update.set("reqCount", entity.getReqCount());
        update.set("errorCount", entity.getErrorCount());
        update.set("rps", entity.getRps());
        update.set("bytes", entity.getBytes());
        update.set("delay", entity.getDelay());
        update.set("dbCost", entity.getDbCost());
        update.set("subMetrics", entity.getSubMetrics());
        update.set("p50", entity.getP50());
        update.set("p95", entity.getP95());
        update.set("p99", entity.getP99());
        update.set("lastCallId", entity.getLastCallId());
        update.set("lastCallId", entity.getLastCallId());
        if (null != entity.getWorkerInfoMap()) {
            update.set("workerInfoMap", entity.getWorkerInfoMap());
        }
        update.currentDate("updatedAt");
        update.set("ttlKey", entity.getTtlKey());
        return update;
    }

    public ApiMetricsRaw findMetricStart(Query query) {
        return mongoTemplate.findOne(query, ApiMetricsRaw.class);
    }
}
