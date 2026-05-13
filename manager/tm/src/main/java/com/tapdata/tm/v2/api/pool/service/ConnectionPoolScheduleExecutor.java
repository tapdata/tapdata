package com.tapdata.tm.v2.api.pool.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Sorts;
import com.tapdata.tm.apiServer.enums.TimeGranularity;
import com.tapdata.tm.base.field.BaseEntityFields;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.entity.ConnectionPoolEntity;
import com.tapdata.tm.worker.entity.field.ConnectionPoolField;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
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

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/4/29 14:51 Create
 * @description
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class ConnectionPoolScheduleExecutor {
    static final int BATCH_READ_SIZE = 1000;
    MongoTemplate mongoTemplate;

    public void aggregateUsage() {
        final String collectionName = MongoUtils.getCollectionNameIgnore(ConnectionPoolEntity.class);
        assert collectionName != null;
        Long lastUpdateTime = lastOne();
        try (ConnectionPoolInstanceFactory acceptor = new ConnectionPoolInstanceFactory(this::saveApiMetricsRaw, this::findMetricStart)) {
            final Criteria criteria = new Criteria();
            criteria.and(ConnectionPoolField.TIME_GRANULARITY.field()).is(TimeGranularity.SECOND_FIVE.getType());
            final MongoCollection<Document> collection = mongoTemplate.getCollection(collectionName);
            if (null != lastUpdateTime) {
                criteria.and(ConnectionPoolField.LAST_UPDATE_TIME.field()).gte(lastUpdateTime);
            }
            final FindIterable<Document> iterable =
                    collection.find(Query.query(criteria).getQueryObject(), Document.class)
                            .sort(Sorts.ascending(ConnectionPoolField.LAST_UPDATE_TIME.field()))
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
        Query query = Query.query(Criteria.where(ConnectionPoolField.TIME_GRANULARITY.field()).is(TimeGranularity.HOUR.getType()));
        query.with(Sort.by(Sort.Order.desc(ConnectionPoolField.LAST_UPDATE_TIME.field()))).limit(1);
        ConnectionPoolEntity lastOne = mongoTemplate.findOne(query, ConnectionPoolEntity.class);
        if (null != lastOne) {
            return lastOne.getLastUpdateTime();
        }
        return null;
    }


    public void saveApiMetricsRaw(List<ConnectionPoolEntity> apiMetricsRawList) {
        if (CollectionUtils.isEmpty(apiMetricsRawList)) {
            return;
        }
        bulkUpsert(apiMetricsRawList);
    }

    public void bulkUpsert(List<ConnectionPoolEntity> entities) {
        bulkUpsert(entities, this::buildDefaultQuery, this::buildDefaultUpdate);
    }


    public void bulkUpsert(List<ConnectionPoolEntity> entities,
                           Function<ConnectionPoolEntity, Query> queryBuilder,
                           Function<ConnectionPoolEntity, Update> updateBuilder) {
        try {
            if (entities == null || entities.isEmpty()) {
                return;
            }
            BulkOperations bulkOps = mongoTemplate.bulkOps(BulkOperations.BulkMode.ORDERED, ConnectionPoolEntity.class);
            for (ConnectionPoolEntity entity : entities) {
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

    private Query buildDefaultQuery(ConnectionPoolEntity entity) {
        Criteria criteria = Criteria.where(ConnectionPoolField.PROCESS_ID.field()).is(entity.getProcessId())
                .and(ConnectionPoolField.CONNECTION_ID.field()).is(entity.getConnectionId())
                .and(ConnectionPoolField.TIME_GRANULARITY.field()).is(entity.getTimeGranularity())
                .and(ConnectionPoolField.LAST_UPDATE_TIME.field()).is(entity.getLastUpdateTime());
        return Query.query(criteria);
    }

    private Update buildDefaultUpdate(ConnectionPoolEntity entity) {
        Update update = new Update();
        Optional.ofNullable(entity.getMaxConnections()).ifPresent(v -> update.set(ConnectionPoolField.MAX_CONNECTIONS.field(), v));
        Optional.ofNullable(entity.getUsedConnections()).ifPresent(v -> update.set(ConnectionPoolField.USED_CONNECTIONS.field(), v));
        Optional.ofNullable(entity.getAvailable()).ifPresent(v -> update.set(ConnectionPoolField.AVAILABLE.field(), v));
        Optional.ofNullable(entity.getQueueSize()).ifPresent(v -> update.set(ConnectionPoolField.QUEUE_SIZE.field(), v));
        Optional.ofNullable(entity.getTtlKey()).ifPresent(v -> update.set(ConnectionPoolField.TTL_KEY.field(), v));
        update.currentDate(BaseEntityFields.UPDATED_AT.field());
        return update;
    }

    public ConnectionPoolEntity findMetricStart(Query query) {
        return mongoTemplate.findOne(query, ConnectionPoolEntity.class);
    }
}
