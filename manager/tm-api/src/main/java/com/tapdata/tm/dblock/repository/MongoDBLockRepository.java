package com.tapdata.tm.dblock.repository;

import com.tapdata.tm.dblock.DBLock;
import com.tapdata.tm.dblock.DBLockRepository;
import com.tapdata.tm.dblock.LockStateEnums;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * MongoDB 数据库锁-持久化实现
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/8/8 12:23 Create
 */
@Component
public class MongoDBLockRepository implements DBLockRepository {

    private final static Class<MongoDBLockEntity> entityClass = MongoDBLockEntity.class;
    private final MongoTemplate operations;

    public MongoDBLockRepository(MongoTemplate operations) {
        this.operations = operations;
        // 初始化表及索引
        if (!operations.collectionExists(entityClass)) {
            operations.createCollection(entityClass);
            operations.indexOps(entityClass).createIndex(new Index(MongoDBLockEntity.FIELD_OWNER, Sort.Direction.ASC));
            operations.indexOps(entityClass).createIndex(new Index(MongoDBLockEntity.FIELD_EXPIRED, Sort.Direction.DESC));
        }
    }

    @Override
    public boolean init(String key) {
        Date currentDate = new Date();

        MongoDBLockEntity entity = operations.findAndModify(
            Query.query(Criteria.where(MongoDBLockEntity.FIELD_LOCK_KEY).is(key))
            , new Update()
                .setOnInsert(MongoDBLockEntity.FIELD_LOCK_KEY, key)
                .setOnInsert(MongoDBLockEntity.FIELD_OWNER, DBLock.NONE_OWNER)     // 锁拥有者：空
                .setOnInsert(MongoDBLockEntity.FIELD_EXPIRED, DBLock.NONE_EXPIRE)  // 过期时间：1970-01-01 00:00:00
                .setOnInsert(MongoDBLockEntity.FIELD_UPDATED, currentDate)
                .setOnInsert(MongoDBLockEntity.FIELD_CREATED, currentDate)
            , FindAndModifyOptions.options()
                .returnNew(false)  // 不返回新对象，用于判断是否重复初始化
                .upsert(true)      // 不存在，创建新对象，用于判断是否重复初始化
            , entityClass);
        return null == entity;
    }

    @Override
    public LockStateEnums renew(String key, String owner, Date expireTime) {
        Date currentDate = new Date();

        MongoDBLockEntity entity = operations.findAndModify(Query.query(Criteria
                .where(MongoDBLockEntity.FIELD_LOCK_KEY).is(key)
                .orOperator(
                    Criteria.where(MongoDBLockEntity.FIELD_EXPIRED).lt(currentDate),     // 锁到期
                    Criteria.where(MongoDBLockEntity.FIELD_OWNER).is(owner)              // 当前锁，可更新
                )
            ), Update
                .update(MongoDBLockEntity.FIELD_UPDATED, currentDate) // 更新时间
                .set(MongoDBLockEntity.FIELD_EXPIRED, expireTime)     // 过期时间
                .set(MongoDBLockEntity.FIELD_OWNER, owner)            // 锁拥有者
            , FindAndModifyOptions.options()
                .returnNew(false) // 不返回新对象，用于判断是否切换拥有者
                .upsert(false)    // 不创建新对象，用于判断是否切换拥有者
            , entityClass);

        if (entity == null) {
            return LockStateEnums.NO;
        } else if (owner.equals(entity.getOwner())) {
            return LockStateEnums.YES;
        } else {
            return LockStateEnums.YES_CHANGE;
        }
    }

    @Override
    public boolean release(String key, String owner) {
        MongoDBLockEntity entity = operations.findAndModify(Query.query(Criteria
                .where(MongoDBLockEntity.FIELD_LOCK_KEY).is(key)
                .and(MongoDBLockEntity.FIELD_OWNER).is(owner)
            ), Update
                .update(MongoDBLockEntity.FIELD_UPDATED, new Date())       // 更新时间
                .set(MongoDBLockEntity.FIELD_EXPIRED, DBLock.NONE_EXPIRE)  // 过期时间：1970-01-01 00:00:00
                .set(MongoDBLockEntity.FIELD_OWNER, DBLock.NONE_OWNER)     // 锁拥有者：空
            , FindAndModifyOptions.options()
                .returnNew(false)
                .upsert(false)
            , entityClass);
        return entity != null;
    }
}
