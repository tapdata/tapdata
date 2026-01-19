package com.tapdata.tm.v2.api.monitor.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Sorts;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.service.WorkerCallServiceImpl;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
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
import org.springframework.util.CollectionUtils;

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

    MetricInstanceFactory create() {
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
            final Query query = Query.query(criteria);
            final Document queryObject = query.getQueryObject();
            final FindIterable<Document> iterable =
                    collection.find(queryObject, Document.class)
                            .sort(Sorts.ascending("reqTime"))
                            .batchSize(1000);
            try (final MongoCursor<Document> cursor = iterable.iterator()) {
                while (cursor.hasNext()) {
                    final Document entity = cursor.next();
                    acceptor.accept(entity);
                }
            }
        }
    }

    ObjectId lastOne() {
        Query query = Query.query(Criteria.where("timeGranularity").is(2));
        query.with(Sort.by(Sort.Order.desc("callId"))).limit(1);
        ApiMetricsRaw lastOne = mongoTemplate.findOne(query, ApiMetricsRaw.class);
        if (null != lastOne) {
            return lastOne.getCallId();
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
                .and(WorkerCallServiceImpl.Tag.API_ID).is(entity.getApiId());
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
        update.set("callId", entity.getCallId());
        update.currentDate("updatedAt");
        return update;
    }

    public ApiMetricsRaw findMetricStart(Query query) {
        return mongoTemplate.findOne(query, ApiMetricsRaw.class);
    }
}
