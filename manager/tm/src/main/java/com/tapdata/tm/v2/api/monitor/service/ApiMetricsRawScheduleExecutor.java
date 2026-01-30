package com.tapdata.tm.v2.api.monitor.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Sorts;
import com.tapdata.tm.apiCalls.entity.ApiCallEntity;
import com.tapdata.tm.apiCalls.entity.ApiCallField;
import com.tapdata.tm.base.dto.ValueResult;
import com.tapdata.tm.base.field.BaseEntityFields;
import com.tapdata.tm.base.field.CollectionField;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.v2.api.monitor.main.entity.ApiMetricsRaw;
import com.tapdata.tm.v2.api.monitor.main.enums.ApiMetricsRawFields;
import com.tapdata.tm.v2.api.monitor.main.enums.MetricTypes;
import com.tapdata.tm.apiServer.enums.TimeGranularity;
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
    static final String MAX_LAST_CALL_ID = "maxLastCallId";
    static final String MIN_OF_MAX_LAST_CALL_ID = "minOfMaxLastCallId";
    static final int BATCH_READ_SIZE = 2000;
    MongoTemplate mongoTemplate;

    public MetricInstanceFactory create() {
        return new MetricInstanceFactory(this::saveApiMetricsRaw, this::findMetricStart);
    }

    public void aggregateApiCall() {
        final String collectionName = MongoUtils.getCollectionNameIgnore(ApiCallEntity.class);
        if (StringUtils.isBlank(collectionName)) {
            return;
        }
        Long lastCallTime = lastOne();
        long queryTime = System.currentTimeMillis();
        try (MetricInstanceFactory acceptor = create().last(lastCallTime)) {
            final MongoCollection<Document> collection = mongoTemplate.getCollection(collectionName);
            final Criteria criteria = Criteria.where(ApiCallField.DELETE.field()).ne(true);
            Criteria newOr;
            if (Objects.nonNull(lastCallTime)) {
                newOr = Criteria.where(ApiCallField.REQ_TIME.field()).gte(lastCallTime).lt(queryTime);
            } else {
                newOr = Criteria.where(ApiCallField.REQ_TIME.field()).lt(queryTime);
            }
            criteria.orOperator(newOr, Criteria.where(ApiCallField.SUPPLEMENT.field()).is(true));
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
            ObjectId batchEnd = null;
            try (final MongoCursor<Document> cursor = iterable.iterator()) {
                while (cursor.hasNext()) {
                    final Document entity = cursor.next();
                    acceptor.accept(entity);
                    ObjectId oId = entity.getObjectId(BaseEntityFields._ID.field());
                    if (null != oId && (null == batchEnd || batchEnd.compareTo(oId) < 0)) {
                        batchEnd = oId;
                    }
                }
            }
            if (null != batchEnd) {
                Criteria lte = Criteria.where(BaseEntityFields._ID.field()).lte(batchEnd)
                        .and(ApiCallField.SUPPLEMENT.field()).is(true);
                Query queried = Query.query(lte);
                mongoTemplate.updateMulti(queried, Update.update(ApiCallField.SUPPLEMENT.field(), false), collectionName);
            }
        }
    }

    protected Long lastOne() {
        final String collectionName = MongoUtils.getCollectionNameIgnore(ApiMetricsRaw.class);
        if (StringUtils.isBlank(collectionName)) {
            return null;
        }
        List<Document> pipeline = Arrays.asList(
                new Document(ValueResult.$MATCH.as(),
                        new Document(ApiMetricsRawFields.TIME_GRANULARITY.field(), TimeGranularity.MINUTE.getType())
                                .append(ApiMetricsRawFields.METRIC_TYPE.field(), MetricTypes.API_SERVER.getType())
                ),
                new Document(ValueResult.$GROUP.as(),
                        new Document(BaseEntityFields._ID.field(), ValueResult.$.concat(ApiMetricsRawFields.REQ_PATH))
                                .append(MAX_LAST_CALL_ID, new Document(ValueResult.$MAX.as(), ValueResult.$.concat(ApiMetricsRawFields.LAST_CALL_ID)))
                ),
                new Document(ValueResult.$GROUP.as(),
                        new Document(BaseEntityFields._ID.field(), null)
                                .append(MIN_OF_MAX_LAST_CALL_ID, new Document(ValueResult.$MIN.as(), ValueResult.$.concat(MAX_LAST_CALL_ID)))
                )
        );
        List<Document> results = mongoTemplate.getCollection(collectionName)
                .aggregate(pipeline)
                .into(new ArrayList<>());
        if (!results.isEmpty()) {
            Document resultDoc = results.get(ValueResult.ZERO_INT.as());
            Object minValue = resultDoc.get(MIN_OF_MAX_LAST_CALL_ID);
            if (minValue instanceof org.bson.types.ObjectId oid) {
                ApiCallEntity lastOne = mongoTemplate.findById(oid, ApiCallEntity.class);
                if (null == lastOne) {
                    return null;
                }
                return TimeGranularity.HOUR.fixTime(lastOne.getReqTime() / 1000L) * 1000L;
            }
            return null;
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
        update.set(ApiMetricsRawFields.LAST_CALL_ID.field(), entity.getLastCallId());
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
