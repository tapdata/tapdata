package com.tapdata.tm.v2.api.usage.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Sorts;
import com.tapdata.tm.base.field.BaseEntityFields;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.worker.entity.ServerUsage;
import com.tapdata.tm.worker.entity.ServerUsageMetric;
import com.tapdata.tm.worker.entity.field.ServerUsageField;
import com.tapdata.tm.worker.entity.field.ServerUsageMetricField;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
public class ServerUsageMetricScheduleExecutor {
    static final int BATCH_READ_SIZE = 1000;
    MongoTemplate mongoTemplate;
    MongoTemplate mongoOperations;

    public void aggregateUsage() {
        final String collectionName = MongoUtils.getCollectionNameIgnore(ServerUsage.class);
        if (StringUtils.isBlank(collectionName)) {
            return;
        }
        Long lastUpdateTime = lastOne();
        try (ServerUsageMetricInstanceFactory acceptor = new ServerUsageMetricInstanceFactory(this::saveApiMetricsRaw, this::findMetricStart)) {
            final MongoCollection<Document> collection = mongoTemplate.getCollection(collectionName);
            final Criteria criteria = new Criteria();
            if (Objects.nonNull(lastUpdateTime)) {
                criteria.and(ServerUsageField.LAST_UPDATE_TIME.field()).gt(lastUpdateTime);
            }
            final Document queryObject = Query.query(criteria).getQueryObject();
            final FindIterable<Document> iterable =
                    collection.find(queryObject, Document.class)
                            .sort(Sorts.ascending(ServerUsageField.LAST_UPDATE_TIME.field()))
                            .batchSize(BATCH_READ_SIZE);
            try (final MongoCursor<Document> cursor = iterable.iterator()) {
                while (cursor.hasNext()) {
                    final Document entity = cursor.next();
                    acceptor.accept(entity);
                }
            }
        }
    }

    Long lastOne() {
        Query query = Query.query(Criteria.where(ServerUsageMetricField.TIME_GRANULARITY.field()).is(TimeGranularity.HOUR.getSeconds()));
        query.with(Sort.by(Sort.Order.desc(ServerUsageField.LAST_UPDATE_TIME.field()))).limit(1);
        ServerUsageMetric lastOne = mongoTemplate.findOne(query, ServerUsageMetric.class);
        if (null != lastOne) {
            return lastOne.getLastUpdateTime();
        }
        return null;
    }


    public void saveApiMetricsRaw(List<ServerUsageMetric> apiMetricsRawList) {
        if (CollectionUtils.isEmpty(apiMetricsRawList)) {
            return;
        }
        bulkUpsert(apiMetricsRawList);
    }

    public void bulkUpsert(List<ServerUsageMetric> entities) {
        bulkUpsert(entities, this::buildDefaultQuery, this::buildDefaultUpdate);
    }


    public void bulkUpsert(List<ServerUsageMetric> entities,
                           Function<ServerUsageMetric, Query> queryBuilder,
                           Function<ServerUsageMetric, Update> updateBuilder) {
        try {
            if (entities == null || entities.isEmpty()) {
                return;
            }
            BulkOperations bulkOps = mongoOperations.bulkOps(BulkOperations.BulkMode.ORDERED, ServerUsageMetric.class);
            for (ServerUsageMetric entity : entities) {
                entity.setTtlKey(new Date(entity.getLastUpdateTime()));
                Query query = queryBuilder.apply(entity);
                Update update = updateBuilder.apply(entity);
                bulkOps.upsert(query, update);
            }
            bulkOps.execute();
        } catch (Exception e) {
            log.error("bulkUpsert ServerUsageMetric error", e);
        }
    }

    private Query buildDefaultQuery(ServerUsageMetric entity) {
        Criteria criteria = Criteria.where(ServerUsageField.LAST_UPDATE_TIME.field()).is(entity.getLastUpdateTime())
                .and(ServerUsageMetricField.TIME_GRANULARITY.field()).is(entity.getTimeGranularity())
                .and(ServerUsageField.PROCESS_ID.field()).is(entity.getProcessId())
                .and(ServerUsageField.WORK_OID.field()).is(entity.getWorkOid());
        return Query.query(criteria);
    }

    private Update buildDefaultUpdate(ServerUsageMetric entity) {
        Update update = new Update();
        update.set(ServerUsageField.PROCESS_TYPE.field(), entity.getProcessType());
        Optional.ofNullable(entity.getMaxCpuUsage())
                .map(BigDecimal::valueOf)
                .map(v -> v.setScale(4, RoundingMode.HALF_DOWN).doubleValue())
                .ifPresent(v -> update.set(ServerUsageMetricField.MAX_CPU_USAGE.field(), v));
        Optional.ofNullable(entity.getMinCpuUsage())
                .map(BigDecimal::valueOf)
                .map(v -> v.setScale(4, RoundingMode.HALF_DOWN).doubleValue())
                .ifPresent(v -> update.set(ServerUsageMetricField.MIN_CPU_USAGE.field(), v));
        Optional.ofNullable(entity.getCpuUsage())
                .map(BigDecimal::valueOf)
                .map(v -> v.setScale(4, RoundingMode.HALF_DOWN).doubleValue())
                .ifPresent(v -> update.set(ServerUsageField.CPU_USAGE.field(), v));
        update.set(ServerUsageMetricField.MAX_HEAP_MEMORY_USAGE.field(), entity.getMaxHeapMemoryUsage());
        update.set(ServerUsageMetricField.MIN_HEAP_MEMORY_USAGE.field(), entity.getMinHeapMemoryUsage());
        update.set(ServerUsageField.HEAP_MEMORY_USAGE.field(), entity.getHeapMemoryUsage());
        update.set(ServerUsageField.HEAP_MEMORY_MAX.field(), entity.getHeapMemoryMax());
        update.set(ServerUsageField.TTL_KEY.field(), entity.getTtlKey());
        update.currentDate(BaseEntityFields.UPDATED_AT.field());
        return update;
    }

    public ServerUsageMetric findMetricStart(Query query) {
        return mongoTemplate.findOne(query, ServerUsageMetric.class);
    }
}
