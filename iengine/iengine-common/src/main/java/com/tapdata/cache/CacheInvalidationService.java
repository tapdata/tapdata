package com.tapdata.cache;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

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
    private final ScheduledExecutorService scheduler;
    private final AtomicLong lastCheckTimestamp = new AtomicLong(System.currentTimeMillis());

    @Getter
    private volatile boolean running = false;
    
    public CacheInvalidationService(HazelcastInstance hazelcastInstance, MongoClient mongoClient, String databaseName) {
        this.hazelcastInstance = hazelcastInstance;
        this.mongoClient = mongoClient;
        this.databaseName = databaseName;
        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "cache-invalidation-checker");
            t.setDaemon(true);
            return t;
        });
        
        ensureIndexes();
        logger.info("Cache invalidation service initialized for database: {}", databaseName);
    }
    
    /**
     * 确保 MongoDB 集合有正确的索引
     */
    private void ensureIndexes() {
        try {
            MongoCollection<Document> collection = mongoClient.getDatabase(databaseName).getCollection(COLLECTION_NAME);
            
            // 创建时间戳索引（用于查询）
            collection.createIndex(Indexes.ascending("timestamp"));
            
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
     */
    private void checkAndEvictCache() {
        try {
            long lastCheck = lastCheckTimestamp.get();
            long now = System.currentTimeMillis();
            
            // 查询新的失效事件
            List<Document> invalidations = mongoClient.getDatabase(databaseName)
                .getCollection(COLLECTION_NAME)
                .find(Filters.gt("timestamp", lastCheck))
                .limit(BATCH_SIZE)
                .into(new ArrayList<>());
            
            if (invalidations.isEmpty()) {
                return;
            }
            
            logger.debug("Found {} cache invalidation events", invalidations.size());
            
            // 处理失效事件
            for (Document inv : invalidations) {
                String mapName = inv.getString("mapName");
                String key = inv.getString("key");
                
                if (mapName != null && key != null) {
                    evictCache(mapName, key);
                }
            }
            
            // 更新最后检查时间
            lastCheckTimestamp.set(now);
            
        } catch (Exception e) {
            logger.error("Error checking cache invalidation events", e);
        }
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

