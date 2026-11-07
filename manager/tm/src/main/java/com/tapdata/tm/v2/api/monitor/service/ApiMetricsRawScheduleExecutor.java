package com.tapdata.tm.v2.api.monitor.service;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoException;
import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;
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
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

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
        long queryTime = System.currentTimeMillis() - 5000L;
        ClientSessionOptions sessionOptions = ClientSessionOptions.builder()
                .defaultTimeout(30, TimeUnit.SECONDS)
                .build();
        while (aggregateApiCallBatch(queryTime, collectionName, sessionOptions) == BATCH_READ_SIZE) {
            // Each committed batch is visible to the fresh factory used for the next batch.
        }
    }

    private int aggregateApiCallBatch(long queryTime, String collectionName, ClientSessionOptions sessionOptions) {
        TransactionOptions transactionOptions = TransactionOptions.builder().build();
        try (ClientSession session = mongoTemplate.getMongoDatabaseFactory().getSession(sessionOptions)) {
            return session.withTransaction(() -> {
                try {
                    return mongoTemplate.withSession(() -> session).execute(operations -> {
                        List<Object> processedIds = new ArrayList<>(BATCH_READ_SIZE);
                        try (MetricInstanceFactory acceptor = create(queryTime, operations)) {
                            eachApi(queryTime, collectionName, operations, acceptor, processedIds);
                        }
                        if (!processedIds.isEmpty()) {
                            Criteria updateCriteria = Criteria.where(BaseEntityFields._ID.field()).in(processedIds)
                                    .and(ApiCallField.HAS_METRIC.field()).is(false);
                            operations.updateMulti(Query.query(updateCriteria),
                                    Update.update(ApiCallField.HAS_METRIC.field(), true), collectionName);
                        }
                        return processedIds.size();
                    });
                } catch (RuntimeException e) {
                    throw retryableMongoCause(e);
                }
            }, transactionOptions);
        }
    }

    private RuntimeException retryableMongoCause(RuntimeException exception) {
        Throwable cause = exception;
        while (cause != null) {
            if (cause instanceof MongoException mongoException
                    && mongoException.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                return mongoException;
            }
            Throwable next = cause.getCause();
            if (next == cause) {
                break;
            }
            cause = next;
        }
        return exception;
    }

    MetricInstanceFactory create(long queryEnd, MongoOperations operations) {
        return new MetricInstanceFactory(
                entities -> saveApiMetricsRaw(operations, entities),
                query -> findMetricStart(operations, query));
    }

    void eachApi(long queryTime, String collectionName, MongoOperations operations,
                 MetricInstanceFactory acceptor, List<Object> processedIds) {
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
                ApiCallField.PHYSICAL_NODE_ID,
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
        query.with(Sort.by(Sort.Direction.ASC,
                ApiCallField.REQ_TIME.field(), BaseEntityFields._ID.field()));
        query.limit(BATCH_READ_SIZE);
        query.cursorBatchSize(BATCH_READ_SIZE);
        try (Stream<Document> cursor = operations.stream(query, Document.class, collectionName)) {
            cursor.forEach(entity -> {
                Object id = entity.get(BaseEntityFields._ID.field());
                if (id == null) {
                    throw new IllegalStateException("Pending ApiCall document has no _id");
                }
                acceptor.accept(entity);
                processedIds.add(id);
            });
        }
    }

    void saveApiMetricsRaw(List<ApiMetricsRaw> apiMetricsRawList) {
        if (CollectionUtils.isEmpty(apiMetricsRawList)) {
            return;
        }
        bulkUpsert(apiMetricsRawList);
    }

    void saveApiMetricsRaw(MongoOperations operations, List<ApiMetricsRaw> apiMetricsRawList) {
        if (CollectionUtils.isEmpty(apiMetricsRawList)) {
            return;
        }
        bulkUpsert(operations, apiMetricsRawList, this::buildDefaultQuery, this::buildDefaultUpdate);
    }

    void bulkUpsert(List<ApiMetricsRaw> entities) {
        bulkUpsert(mongoTemplate, entities, this::buildDefaultQuery, this::buildDefaultUpdate);
    }

    void bulkUpsert(List<ApiMetricsRaw> entities,
                    Function<ApiMetricsRaw, Query> queryBuilder,
                    Function<ApiMetricsRaw, Update> updateBuilder) {
        bulkUpsert(mongoTemplate, entities, queryBuilder, updateBuilder);
    }

    void bulkUpsert(MongoOperations operations, List<ApiMetricsRaw> entities,
                    Function<ApiMetricsRaw, Query> queryBuilder,
                    Function<ApiMetricsRaw, Update> updateBuilder) {
        try {
            BulkOperations bulkOps = operations.bulkOps(BulkOperations.BulkMode.ORDERED, ApiMetricsRaw.class);
            for (ApiMetricsRaw entity : entities) {
                if (entity.getDelay().size() > 50_000
                        || entity.getDbCost().size() > 50_000) {
                    throw new IllegalStateException("API metric record exceeds the Raw storage cap: delay="
                            + entity.getDelay().size() + ", dbCost=" + entity.getDbCost().size());
                }
                entity.setTtlKey(new Date(entity.getTimeStart() * 1000L));
                Query query = queryBuilder.apply(entity);
                Update update = updateBuilder.apply(entity);
                bulkOps.upsert(query, update);
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
                .and(ApiMetricsRawFields.PHYSICAL_NODE_ID.field()).is(entity.getPhysicalNodeId())
                .and(ApiMetricsRawFields.API_ID.field()).is(entity.getApiId())
                .and(ApiMetricsRawFields.REQ_PATH.field()).is(entity.getReqPath())
                .and(ApiMetricsRawFields.METRIC_TYPE.field()).is(entity.getMetricType());
        return Query.query(criteria);
    }

    protected Update buildDefaultUpdate(ApiMetricsRaw entity) {
        Update update = new Update();
        update.set(ApiMetricsRawFields.REQ_COUNT.field(), entity.getReqCount());
        update.set(ApiMetricsRawFields.PHYSICAL_NODE_ID.field(), entity.getPhysicalNodeId());
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

    ApiMetricsRaw findMetricStart(MongoOperations operations, Query query) {
        return operations.findOne(query, ApiMetricsRaw.class);
    }
}
