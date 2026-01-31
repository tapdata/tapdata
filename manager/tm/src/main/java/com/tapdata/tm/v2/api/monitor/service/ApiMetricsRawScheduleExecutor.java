package com.tapdata.tm.v2.api.monitor.service;

import com.mongodb.ClientSessionOptions;
import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Sorts;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.entity.ApiCallField;
import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.base.field.BaseEntityFields;
import com.tapdata.tm.base.field.CollectionField;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.ApiMetricsRawFields;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
    static final int BATCH_READ_SIZE = 2000;

    MongoTemplate mongoTemplate;

    public MetricInstanceFactory create(long queryEnd) {
        return new MetricInstanceFactory(this::saveApiMetricsRaw, this::findMetricStart);
    }

    public void aggregateApiCall() {
        String collectionName = MongoUtils.getCollectionNameIgnore(ApiCallEntity.class);
        assert null != collectionName;
        long queryTime = System.currentTimeMillis() - 1000L;
        ClientSessionOptions sessionOptions = ClientSessionOptions.builder()
                .defaultTimeout(30, TimeUnit.SECONDS)
                .defaultTransactionOptions(TransactionOptions.builder().build())
                .build();
        ClientSession session = null;
        try {
            session = mongoTemplate.getMongoDatabaseFactory().getSession(sessionOptions);
            session.startTransaction();
            try (MetricInstanceFactory acceptor = create(queryTime)) {
                final MongoCollection<Document> collection = mongoTemplate.getCollection(collectionName);
                eachApi(queryTime, collection, acceptor);
            }
            Criteria updateCriteria = Criteria.where(BaseEntityFields.CREATE_TIME.field()).lt(new Date(queryTime))
                    .and(ApiCallField.HAS_METRIC.field()).is(false);
            Query queried = Query.query(updateCriteria);
            mongoTemplate.updateMulti(queried, Update.update(ApiCallField.HAS_METRIC.field(), true), "ApiCall");
            session.commitTransaction();
        } catch (Exception e) {
            log.error("bulkUpsert ApiMetricsRaw error", e);
            if (session != null && session.hasActiveTransaction()) {
                session.abortTransaction();
            }
            throw e;
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    void eachApi(long queryTime, MongoCollection<Document> collection, MetricInstanceFactory acceptor) {
        final Criteria criteria = Criteria.where(BaseEntityFields.CREATE_TIME.field()).lt(new Date(queryTime))
                .and(ApiCallField.HAS_METRIC.field()).is(false)
                .and(ApiCallField.DELETE.field()).ne(true);
        final Query query = Query.query(criteria);
        String[] filterFields = CollectionField.fields(
                BaseEntityFields._ID,
                ApiCallField.ALL_PATH_ID,
                ApiCallField.WORK_O_ID,
                ApiCallField.REQ_PATH,
                ApiCallField.API_GATEWAY_UUID,
                ApiCallField.LATENCY,
                ApiCallField.REQ_BYTES,
                ApiCallField.REQ_TIME,
                ApiCallField.CODE,
                ApiCallField.HTTP_STATUS,
                BaseEntityFields.CREATE_TIME,
                ApiCallField.DATA_QUERY_TOTAL_TIME,
                ApiCallField.WORK_O_ID,
                ApiCallField.REQ_PATH,
                ApiCallField.SUPPLEMENT,
                ApiCallField.SUCCEED
        );
        query.fields().include(filterFields);
        final Document queryObject = query.getQueryObject();
        final FindIterable<Document> iterable =
                collection.find(queryObject, Document.class)
                        .sort(Sorts.ascending(ApiCallField.REQ_TIME.field()))
                        .batchSize(BATCH_READ_SIZE);
        try (final MongoCursor<Document> cursor = iterable.iterator()) {
            while (cursor.hasNext()) {
                final Document entity = cursor.next();
                acceptor.accept(entity);
            }
            Criteria updateCriteria = Criteria.where(BaseEntityFields.CREATE_TIME.field()).lt(new Date(queryTime))
                    .and(ApiCallField.HAS_METRIC.field()).is(false);
            Query queried = Query.query(updateCriteria);
            mongoTemplate.updateMulti(queried, Update.update(ApiCallField.HAS_METRIC.field(), true), "ApiCall");
        }
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
            long maxTime = 0L;
            for (ApiMetricsRaw entity : entities) {
                entity.setTtlKey(new Date(entity.getTimeStart() * 1000L));
                Query query = queryBuilder.apply(entity);
                Update update = updateBuilder.apply(entity);
                bulkOps.upsert(query, update);
                if (entity.getTimeGranularity() == TimeGranularity.MINUTE.getType()) {
                    Map<Long, ApiMetricsRaw> subMetrics = entity.getSubMetrics();
                    for (ApiMetricsRaw sub : subMetrics.values()) {
                        maxTime = Math.max(maxTime, sub.getTimeStart());
                    }
                }
            }
            bulkOps.execute();
        } catch (Exception e) {
            log.error("bulkUpsert ApiMetricsRaw error", e);
            throw e;
        }
    }

    protected Query buildDefaultQuery(ApiMetricsRaw entity) {
        Criteria criteria = Criteria.where(ApiMetricsRawFields.TIME_START.field()).is(entity.getTimeStart())
                .and(ApiMetricsRawFields.TIME_GRANULARITY.field()).is(entity.getTimeGranularity())
                .and(ApiMetricsRawFields.PROCESS_ID.field()).is(entity.getProcessId())
                .and(ApiMetricsRawFields.API_ID.field()).is(entity.getApiId())
                .and(ApiMetricsRawFields.REQ_PATH.field()).is(entity.getReqPath())
                .and(ApiMetricsRawFields.METRIC_TYPE.field()).is(entity.getMetricType());
        return Query.query(criteria);
    }

    protected Update buildDefaultUpdate(ApiMetricsRaw entity) {
        Update update = new Update();
        update.set(ApiMetricsRawFields.REQ_COUNT.field(), entity.getReqCount());
        update.set(ApiMetricsRawFields.ERROR_COUNT.field(), entity.getErrorCount());
        update.set(ApiMetricsRawFields.RPS.field(), entity.getRps());
        update.set(ApiMetricsRawFields.BYTES.field(), entity.getBytes());
        update.set(ApiMetricsRawFields.DELAY.field(), entity.getDelay());
        update.set(ApiMetricsRawFields.DB_COST.field(), entity.getDbCost());
        update.set(ApiMetricsRawFields.SUB_METRICS.field(), entity.getSubMetrics());
        update.set(ApiMetricsRawFields.P50.field(), entity.getP50());
        update.set(ApiMetricsRawFields.P95.field(), entity.getP95());
        update.set(ApiMetricsRawFields.P99.field(), entity.getP99());
        if (null != entity.getWorkerInfoMap()) {
            update.set(ApiMetricsRawFields.WORKER_INFO_MAP.field(), entity.getWorkerInfoMap());
        }
        update.currentDate(BaseEntityFields.LAST_UPDATED.field());
        update.set(ApiMetricsRawFields.TTL_KEY.field(), entity.getTtlKey());
        return update;
    }

    public ApiMetricsRaw findMetricStart(Query query) {
        return mongoTemplate.findOne(query, ApiMetricsRaw.class);
    }
}
