package com.tapdata.cache;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Sorts;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import org.bson.types.ObjectId;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 缓存失效通知服务
 * 用于在独立 Hazelcast 集群间通过 MongoDB 同步缓存失效事件
 * 
 * @author Tapdata
 */
public class CacheInvalidationService {
    
    private static final Logger logger = LogManager.getLogger(CacheInvalidationService.class);
    private static final String COLLECTION_NAME = "CacheInvalidations";
    private static final int CHECK_INTERVAL_SECONDS = 5; // 每 5 秒检查一次
    private static final int BATCH_SIZE = 1000; // 每次最多处理 1000 条
    
    private final HazelcastInstance hazelcastInstance;
    private final MongoClient mongoClient;
    private final String databaseName;
    private final String localNodeId;
    private final ScheduledExecutorService scheduler;
    // 以 _id (ObjectId) 作为游标, 避免基于客户端时间戳带来的边界重复读取与跨节点时钟漂移导致的事件丢失
    private final AtomicReference<ObjectId> lastSeenId = new AtomicReference<>();

    @Getter
    private volatile boolean running = false;
    
    public CacheInvalidationService(HazelcastInstance hazelcastInstance, MongoClient mongoClient, String databaseName, String nodeId) {
        this.hazelcastInstance = hazelcastInstance;
        this.mongoClient = mongoClient;
        this.databaseName = databaseName;
        if (nodeId == null || nodeId.isEmpty()) {
            this.localNodeId = UUID.randomUUID().toString();
            logger.warn("nodeId is blank, fall back to random UUID {}; self-eviction filter may be ineffective", this.localNodeId);
        } else {
            this.localNodeId = nodeId;
        }
        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "cache-invalidation-checker");
            t.setDaemon(true);
            return t;
        });
        
        ensureIndexes();
        logger.info("Cache invalidation service initialized for database: {}, nodeId: {}", databaseName, localNodeId);
    }
    
    /**
     * 确保 MongoDB 集合有正确的索引
     */
    private void ensureIndexes() {
        try {
            MongoCollection<Document> collection = mongoClient.getDatabase(databaseName).getCollection(COLLECTION_NAME);
            
            // 创建 TTL 索引（自动清理 1 分钟前的记录）
            collection.createIndex(
                Indexes.ascending("ttl"),
                new IndexOptions().expireAfter(0L, TimeUnit.SECONDS)
            );
            
            logger.info("Cache invalidation indexes created successfully");
        } catch (Exception e) {
            logger.error("Failed to create cache invalidation indexes", e);
        }
    }
    
    /**
     * 启动失效事件监听
     */
    public void start() {
        if (running) {
            logger.warn("Cache invalidation service is already running");
            return;
        }
        
        running = true;
        lastSeenId.compareAndSet(null, findCurrentMaxId());
        scheduler.scheduleAtFixedRate(
            this::checkAndEvictCache,
            0,
            CHECK_INTERVAL_SECONDS,
            TimeUnit.SECONDS
        );
        
        logger.info("Cache invalidation service started, check interval: {} seconds", CHECK_INTERVAL_SECONDS);
    }
    
    /**
     * 停止失效事件监听
     */
    public void stop() {
        if (!running) {
            return;
        }
        
        running = false;
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("Cache invalidation service stopped");
    }
    
    /**
     * 发布缓存失效事件
     * 
     * @param mapName IMap 名称
     * @param key 失效的 key
     */
    public void publishInvalidation(String mapName, String key) {
        try {
            Document invalidation = new Document()
                .append("mapName", mapName)
                .append("key", key)
                .append("nodeId", localNodeId)
                .append("timestamp", System.currentTimeMillis())
                .append("ttl", new Date(System.currentTimeMillis() + 60000)); // 1 分钟后自动删除
            
            mongoClient.getDatabase(databaseName)
                .getCollection(COLLECTION_NAME)
                .insertOne(invalidation);
            
            logger.debug("Published cache invalidation: map={}, key={}", mapName, key);
        } catch (Exception e) {
            logger.error("Failed to publish cache invalidation: map={}, key={}", mapName, key, e);
        }
    }
    
    /**
     * 检查并失效缓存
     *
     * 以 _id (ObjectId) 作为游标, 配合 Filters.gt 实现严格"游标 > lastSeen"语义,
     * 每条文档至多被本节点处理一次, 不再依赖订阅端 wall-clock, 也不会再次读取边界文档.
     * 注: 完全消除发布端跨节点时钟漂移导致的丢事件问题需要切换到 MongoDB Change Streams,
     *     当前实现已显著降低问题发生概率, 后续可演进.
     */
    private void checkAndEvictCache() {
        try {
            ObjectId cursor = lastSeenId.get();
            int totalProcessed = 0;

            while (totalProcessed < BATCH_SIZE * 10) {
                List<Document> invalidations = mongoClient.getDatabase(databaseName)
                    .getCollection(COLLECTION_NAME)
                    .find(cursor != null ? Filters.gt("_id", cursor) : new Document())
                    .sort(Sorts.ascending("_id"))
                    .limit(BATCH_SIZE)
                    .into(new ArrayList<>());

                if (invalidations.isEmpty()) {
                    break;
                }

                ObjectId maxId = cursor;
                int processedInBatch = 0;

                for (Document inv : invalidations) {
                    ObjectId id = inv.getObjectId("_id");
                    if (id != null && (maxId == null || id.compareTo(maxId) > 0)) {
                        maxId = id;
                    }

                    String mapName = inv.getString("mapName");
                    String key = inv.getString("key");
                    String docNodeId = inv.getString("nodeId");

                    if (mapName == null || key == null) {
                        continue;
                    }
                    // 跳过本节点自己发布的事件, 避免 publisher 把刚写入的本地缓存项再次驱逐
                    if (localNodeId.equals(docNodeId)) {
                        continue;
                    }

                    evictCache(mapName, key);
                    processedInBatch++;
                }

                totalProcessed += processedInBatch;

                if (maxId != null && (cursor == null || maxId.compareTo(cursor) > 0)) {
                    cursor = maxId;
                    lastSeenId.set(cursor);
                }

                if (invalidations.size() < BATCH_SIZE) {
                    break;
                }
            }

            if (totalProcessed > 0) {
                logger.info("Total processed {} cache invalidation events", totalProcessed);
            }

        } catch (Exception e) {
            logger.error("Error checking cache invalidation events", e);
        }
    }

    /**
     * 启动时读取当前集合的最大 _id, 用作游标初值
     */
    private ObjectId findCurrentMaxId() {
        try {
            Document doc = mongoClient.getDatabase(databaseName)
                .getCollection(COLLECTION_NAME)
                .find()
                .sort(Sorts.descending("_id"))
                .limit(1)
                .first();
            if (doc != null) {
                ObjectId id = doc.getObjectId("_id");
                if (id != null) {
                    return id;
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to read max _id from {}, fall back to a fresh ObjectId", COLLECTION_NAME, e);
        }
        return new ObjectId();
    }
    
    /**
     * 失效本地缓存
     */
    private void evictCache(String mapName, String key) {
        try {
            IMap<String, Object> map = hazelcastInstance.getMap(mapName);
            map.evict(key);
            logger.debug("Evicted cache: map={}, key={}", mapName, key);
        } catch (Exception e) {
            logger.error("Failed to evict cache: map={}, key={}", mapName, key, e);
        }
    }
}

