package com.tapdata.tm.v2.api.usage.service;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Sorts;
import com.tapdata.tm.apiCalls.service.WorkerCallServiceImpl;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.entity.ServerUsage;
import com.tapdata.tm.worker.entity.ServerUsageMetric;
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
public class ServerUsageMetricScheduleExecutor {
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
            final Criteria criteria = Criteria.where("deleted").ne(true);
            if (Objects.nonNull(lastUpdateTime)) {
                criteria.and("lastUpdateTime").gt(lastUpdateTime);
            }
            final Document queryObject = Query.query(criteria).getQueryObject();
            final FindIterable<Document> iterable =
                    collection.find(queryObject, Document.class)
                            .sort(Sorts.ascending("lastUpdateTime"))
                            .batchSize(1000);
            try (final MongoCursor<Document> cursor = iterable.iterator()) {
                while (cursor.hasNext()) {
                    final Document entity = cursor.next();
                    acceptor.accept(entity);
                }
            }
        }
    }

    Long lastOne() {
        Query query = Query.query(Criteria.where("timeGranularity").is(2));
        query.with(Sort.by(Sort.Order.desc("lastUpdateTime"))).limit(1);
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
        Criteria criteria = Criteria.where("lastUpdateTime").is(entity.getLastUpdateTime())
                .and(WorkerCallServiceImpl.Tag.TIME_GRANULARITY).is(entity.getTimeGranularity())
                .and(WorkerCallServiceImpl.Tag.PROCESS_ID).is(entity.getProcessId())
                .and("workOid").is(entity.getWorkOid());
        return Query.query(criteria);
    }

    private Update buildDefaultUpdate(ServerUsageMetric entity) {
        Update update = new Update();
        update.set("processType", entity.getProcessType());
        update.set("maxCpuUsage", BigDecimal.valueOf(entity.getMaxCpuUsage()).setScale(4, RoundingMode.HALF_DOWN).doubleValue());
        update.set("minCpuUsage", BigDecimal.valueOf(entity.getMinCpuUsage()).setScale(4, RoundingMode.HALF_DOWN).doubleValue());
        update.set("maxHeapMemoryUsage", entity.getMaxHeapMemoryUsage());
        update.set("minHeapMemoryUsage", entity.getMinHeapMemoryUsage());
        update.set("cpuUsage", BigDecimal.valueOf(entity.getCpuUsage()).setScale(4, RoundingMode.HALF_DOWN).doubleValue());
        update.set("heapMemoryUsage", entity.getHeapMemoryUsage());
        update.set("heapMemoryMax", entity.getHeapMemoryMax());
        update.currentDate("updatedAt");
        return update;
    }

    public ServerUsageMetric findMetricStart(Query query) {
        return mongoTemplate.findOne(query, ServerUsageMetric.class);
    }
}
