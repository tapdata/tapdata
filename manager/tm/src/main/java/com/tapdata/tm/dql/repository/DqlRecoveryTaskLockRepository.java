package com.tapdata.tm.dql.repository;

import com.mongodb.client.result.DeleteResult;
import com.tapdata.tm.dql.entity.DqlRecoveryTaskLockEntity;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.Date;

@Repository
public class DqlRecoveryTaskLockRepository {
    private static final long DEFAULT_LEASE_MILLIS = 1800_000L;
    private static final String OWNER = "tm";

    private final MongoTemplate mongoTemplate;
    private final String collectionName;

    public DqlRecoveryTaskLockRepository(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
        this.collectionName = DqlRecoveryTaskLockEntity.class
                .getAnnotation(Document.class)
                .value();
        init();
    }

    protected void init() {
        if (!mongoTemplate.collectionExists(collectionName)) {
            mongoTemplate.createCollection(collectionName);
        }
        mongoTemplate.indexOps(collectionName).createIndex(new Index()
                .on(DqlRecoveryTaskLockEntity.FIELD_TASK_ID, Sort.Direction.ASC)
                .unique()
                .named("uk_task_id"));
        mongoTemplate.indexOps(collectionName).createIndex(new Index()
                .on(DqlRecoveryTaskLockEntity.FIELD_EXPIRE_AT, Sort.Direction.ASC)
                .named("idx_expire_at"));
    }

    public boolean tryAcquire(String taskId, String batchId) {
        return tryAcquire(taskId, batchId, DEFAULT_LEASE_MILLIS / 1000L);
    }

    public boolean tryAcquire(String taskId, String batchId, long leaseSeconds) {
        if (leaseSeconds <= 0L || leaseSeconds > Long.MAX_VALUE / 1000L) {
            return false;
        }
        Date now = new Date();
        return tryAcquire(taskId, batchId, now,
                new Date(saturatedAdd(now.getTime(), leaseSeconds * 1000L)));
    }

    public boolean tryAcquire(String taskId, String batchId, Date now, Date expireAt) {
        if (StringUtils.isAnyBlank(taskId, batchId) || now == null || expireAt == null
                || !expireAt.after(now)) {
            return false;
        }
        Criteria criteria = Criteria.where(DqlRecoveryTaskLockEntity.FIELD_TASK_ID).is(taskId);
        criteria.orOperator(
                Criteria.where(DqlRecoveryTaskLockEntity.FIELD_EXPIRE_AT).lte(now),
                Criteria.where(DqlRecoveryTaskLockEntity.FIELD_EXPIRE_AT).exists(false));
        Query query = Query.query(criteria);
        Update update = new Update()
                .set(DqlRecoveryTaskLockEntity.FIELD_TASK_ID, taskId)
                .set(DqlRecoveryTaskLockEntity.FIELD_BATCH_ID, batchId)
                .set(DqlRecoveryTaskLockEntity.FIELD_OWNER, OWNER)
                .set(DqlRecoveryTaskLockEntity.FIELD_EXPIRE_AT, expireAt)
                .setOnInsert(DqlRecoveryTaskLockEntity.FIELD_CREATED, now);
        try {
            return mongoTemplate.findAndModify(
                    query,
                    update,
                    FindAndModifyOptions.options().upsert(true).returnNew(true),
                    DqlRecoveryTaskLockEntity.class) != null;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }

    public boolean existsActive(String taskId, Date now) {
        if (StringUtils.isBlank(taskId) || now == null) {
            return false;
        }
        Query query = Query.query(Criteria.where(DqlRecoveryTaskLockEntity.FIELD_TASK_ID).is(taskId)
                .and(DqlRecoveryTaskLockEntity.FIELD_EXPIRE_AT).gt(now));
        return mongoTemplate.exists(query, DqlRecoveryTaskLockEntity.class);
    }

    public DqlRecoveryTaskLockEntity findByTaskId(String taskId) {
        if (StringUtils.isBlank(taskId)) {
            return null;
        }
        Query query = Query.query(Criteria.where(DqlRecoveryTaskLockEntity.FIELD_TASK_ID).is(taskId));
        return mongoTemplate.findOne(query, DqlRecoveryTaskLockEntity.class);
    }

    public boolean release(String taskId, String batchId) {
        if (StringUtils.isAnyBlank(taskId, batchId)) {
            return false;
        }
        Query query = Query.query(Criteria.where(DqlRecoveryTaskLockEntity.FIELD_TASK_ID).is(taskId)
                .and(DqlRecoveryTaskLockEntity.FIELD_BATCH_ID).is(batchId));
        DeleteResult deleted = mongoTemplate.remove(query, DqlRecoveryTaskLockEntity.class);
        return deleted.getDeletedCount() > 0;
    }

    private long saturatedAdd(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }
}
