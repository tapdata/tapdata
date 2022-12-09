package com.tapdata.tm.lock.service.impl;

import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.lock.entity.LockDocument;
import com.tapdata.tm.lock.service.LockService;
import com.tapdata.tm.utils.UUIDUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * @Author: Zed
 * @Date: 2021/12/17
 * @Description: 基于mongodb的分布式锁的实现
 */
@Service
@Slf4j
public class LockServiceImpl implements LockService {

    private ThreadLocal<String> lockValue = new ThreadLocal<>();

    @Autowired
    private MongoTemplate mongoTemplate;

    /**
     *
     * @param key 锁的key
     * @param expiration 过期时间 秒
     * @param sleepMillis 每次睡眠时间 毫秒
     * @return
     */
    @Override
    public boolean lock(String key, long expiration, long sleepMillis) {
        expiration = expiration * 1000;
        int retryTimes = (int) ((expiration) / sleepMillis);
        boolean result = acquire(key, expiration);
        while((!result) && retryTimes-- > 0){
            try {
                log.debug("Lock failed, retrying..." + retryTimes);
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                return false;
            }
            result = acquire(key, expiration);
        }
        return result;
    }


    private boolean acquire(String key, long expiration) {
        Query query = Query.query(Criteria.where("_id").is(key));
        String token = UUIDUtil.getUUID();
        Update update = new Update()
                .setOnInsert("_id", key)
                .setOnInsert("expireAt", System.currentTimeMillis() + expiration)
                .setOnInsert("token", token);

        FindAndModifyOptions options = new FindAndModifyOptions().upsert(true)
                .returnNew(true);
        LockDocument doc = null;
        try {
            doc = mongoTemplate.findAndModify(query, update, options,
                LockDocument.class);
        } catch (Exception e) {
            return false;
        }
        boolean locked = doc != null && doc.getToken() != null && doc.getToken().equals(token);

        // 如果已过期
        if (!locked && doc.getExpireAt() < System.currentTimeMillis()) {
            DeleteResult deleted = this.mongoTemplate.remove(
                    Query.query(Criteria.where("_id").is(key)
                            .and("token").is(doc.getToken())
                            .and("expireAt").is(doc.getExpireAt())),
                    LockDocument.class);
            if (deleted.getDeletedCount() >= 1) {
                // 成功释放锁， 再次尝试获取锁
                locked =  acquire(key, expiration);
            }
        }

        log.debug("Tried to acquire lock for key {} with token {} . Locked: {}",
                key, token, locked);

        if (locked) {
            lockValue.set(token);
        }
        return locked;
    }

    @Override
    public boolean release(String key) {
        try {
            String token = lockValue.get();
            Query query = Query.query(Criteria.where("_id").is(key)
                    .and("token").is(token));
            DeleteResult deleted = mongoTemplate.remove(query, LockDocument.class);
            boolean released = deleted.getDeletedCount() == 1;
            if (released) {
                log.debug("Remove query successfully affected 1 record for key {} with token {}",
                        key, token);
            } else if (deleted.getDeletedCount() > 0) {
                log.error("Unexpected result from release for key {} with token {}, released {}",
                        key, token, deleted);
            } else {
                log.error("Remove query did not affect any records for key {} with token {}",
                        key, token);
            }

            return released;
        } catch (Exception e) {
            log.error("release lock exception {}", e.getMessage());
        } finally {
            lockValue.remove();
        }
        return false;
    }

    @Override
    public boolean refresh(String key, long expiration) {
        String token = lockValue.get();
        Query query = Query.query(Criteria.where("_id").is(key)
                .and("token").is(token));
        Update update = Update.update("expireAt",
                System.currentTimeMillis() + expiration);
        UpdateResult updated =
                mongoTemplate.updateFirst(query, update, LockDocument.class);

        final boolean refreshed = updated.getModifiedCount() == 1;
        if (refreshed) {
            log.debug("Refresh query successfully affected 1 record for key {} " +
                    "with token {}", key, token);
        } else if (updated.getModifiedCount() > 0) {
            log.error("Unexpected result from refresh for key {} with token {}, " +
                    "released {}", key, token, updated);
        } else {
            log.warn("Refresh query did not affect any records for key {} with token {}. " +
                            "This is possible when refresh interval fires for the final time " +
                            "after the lock has been released",
                    key, token);
        }

        return refreshed;
    }

    /**
     * 一天清理一次可能过期没有释放的锁
     */
    @Scheduled(fixedRate = 3600 * 1000 * 24)
    public void clearDoc() {
        long current = System.currentTimeMillis() - (60 * 1000L);
        Query query = new Query(Criteria.where("expireAt").lt(current));
        mongoTemplate.remove(query, LockDocument.class);
    }
}
