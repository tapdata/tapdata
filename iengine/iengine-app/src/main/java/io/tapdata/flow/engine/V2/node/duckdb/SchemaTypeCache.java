package io.tapdata.flow.engine.V2.node.duckdb;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Schema 类型转换缓存
 * 支持 Arrow 类型和 DuckDB 类型的缓存
 * 采用长时间不用按需失效策略
 */
public class SchemaTypeCache {
    
    private static final Logger logger = LoggerFactory.getLogger(SchemaTypeCache.class);
    
    /**
     * 单例实例
     */
    private static volatile SchemaTypeCache instance;
    
    /**
     * Arrow 类型缓存：tableId -> fieldName -> ArrowType
     */
    private final Map<String, Map<String, CacheEntry<ArrowType>>> arrowTypeCache = new ConcurrentHashMap<>();
    
    /**
     * DuckDB 类型缓存：tableId -> fieldName -> String
     */
    private final Map<String, Map<String, CacheEntry<String>>> duckDbTypeCache = new ConcurrentHashMap<>();
    
    /**
     * 缓存过期时间（毫秒），默认 1 小时
     */
    private long cacheExpireTime = TimeUnit.HOURS.toMillis(1);
    
    /**
     * 上次清理时间
     */
    private volatile long lastCleanupTime = System.currentTimeMillis();
    
    /**
     * 清理间隔（毫秒），默认 10 分钟
     */
    private long cleanupInterval = TimeUnit.MINUTES.toMillis(10);
    
    /**
     * 获取单例
     */
    public static SchemaTypeCache getInstance() {
        if (instance == null) {
            synchronized (SchemaTypeCache.class) {
                if (instance == null) {
                    instance = new SchemaTypeCache();
                }
            }
        }
        return instance;
    }
    
    private SchemaTypeCache() {
    }
    
    /**
     * 设置缓存过期时间
     * 
     * @param expireTime 过期时间（毫秒）
     */
    public void setCacheExpireTime(long expireTime) {
        this.cacheExpireTime = expireTime;
    }
    
    /**
     * 设置清理间隔
     * 
     * @param interval 清理间隔（毫秒）
     */
    public void setCleanupInterval(long interval) {
        this.cleanupInterval = interval;
    }
    
    /**
     * 获取或计算 Arrow 类型
     * 
     * @param tableId 表 ID
     * @param fieldName 字段名
     * @param supplier 类型计算函数
     * @return Arrow 类型
     */
    public ArrowType getOrComputeArrowType(String tableId, String fieldName, Supplier<ArrowType> supplier) {
        // 尝试清理过期缓存
        tryCleanup();
        
        // 获取表级缓存
        Map<String, CacheEntry<ArrowType>> tableCache = arrowTypeCache.computeIfAbsent(
            tableId, k -> new ConcurrentHashMap<>()
        );
        
        // 尝试从缓存获取
        CacheEntry<ArrowType> entry = tableCache.get(fieldName);
        if (entry != null && !entry.isExpired(cacheExpireTime)) {
            entry.touch();
            logger.debug("Arrow type cache hit: {}.{}", tableId, fieldName);
            return entry.getValue();
        }
        
        // 缓存未命中，计算类型
        logger.debug("Arrow type cache miss: {}.{}, computing...", tableId, fieldName);
        ArrowType type = supplier.get();
        
        // 缓存结果
        if (type != null) {
            tableCache.put(fieldName, new CacheEntry<>(type));
        }
        
        return type;
    }
    
    /**
     * 获取或计算 DuckDB 类型
     * 
     * @param tableId 表 ID
     * @param fieldName 字段名
     * @param supplier 类型计算函数
     * @return DuckDB 类型字符串
     */
    public String getOrComputeDuckDbType(String tableId, String fieldName, Supplier<String> supplier) {
        // 尝试清理过期缓存
        tryCleanup();
        
        // 获取表级缓存
        Map<String, CacheEntry<String>> tableCache = duckDbTypeCache.computeIfAbsent(
            tableId, k -> new ConcurrentHashMap<>()
        );
        
        // 尝试从缓存获取
        CacheEntry<String> entry = tableCache.get(fieldName);
        if (entry != null && !entry.isExpired(cacheExpireTime)) {
            entry.touch();
            logger.debug("DuckDB type cache hit: {}.{}", tableId, fieldName);
            return entry.getValue();
        }
        
        // 缓存未命中，计算类型
        logger.debug("DuckDB type cache miss: {}.{}, computing...", tableId, fieldName);
        String type = supplier.get();
        
        // 缓存结果
        if (type != null) {
            tableCache.put(fieldName, new CacheEntry<>(type));
        }
        
        return type;
    }
    
    /**
     * 清空指定表的缓存
     * 
     * @param tableId 表 ID
     */
    public void clearTableCache(String tableId) {
        arrowTypeCache.remove(tableId);
        duckDbTypeCache.remove(tableId);
        logger.debug("Cleared cache for table: {}", tableId);
    }
    
    /**
     * 清空所有缓存
     */
    public void clearAll() {
        arrowTypeCache.clear();
        duckDbTypeCache.clear();
        logger.debug("All cache cleared");
    }
    
    /**
     * 尝试清理过期缓存
     */
    private void tryCleanup() {
        long now = System.currentTimeMillis();
        if (now - lastCleanupTime < cleanupInterval) {
            return;
        }
        
        lastCleanupTime = now;
        
        // 清理 Arrow 类型缓存
        int arrowRemoved = cleanCache(arrowTypeCache);
        // 清理 DuckDB 类型缓存
        int duckDbRemoved = cleanCache(duckDbTypeCache);
        
        if (arrowRemoved + duckDbRemoved > 0) {
            logger.debug("Cleaned up {} expired cache entries (Arrow: {}, DuckDB: {})", 
                arrowRemoved + duckDbRemoved, arrowRemoved, duckDbRemoved);
        }
    }
    
    /**
     * 清理指定缓存
     */
    private <T> int cleanCache(Map<String, Map<String, CacheEntry<T>>> cache) {
        int removedCount = 0;
        
        for (Map.Entry<String, Map<String, CacheEntry<T>>> tableEntry : cache.entrySet()) {
            Map<String, CacheEntry<T>> tableCache = tableEntry.getValue();
            
            // 清理表内过期条目
            for (Map.Entry<String, CacheEntry<T>> fieldEntry : tableCache.entrySet()) {
                if (fieldEntry.getValue().isExpired(cacheExpireTime)) {
                    tableCache.remove(fieldEntry.getKey());
                    removedCount++;
                }
            }
            
            // 如果表缓存为空，移除表
            if (tableCache.isEmpty()) {
                cache.remove(tableEntry.getKey());
            }
        }
        
        return removedCount;
    }
    
    /**
     * 缓存条目
     */
    private static class CacheEntry<T> {
        private final T value;
        private volatile long lastAccessTime;
        private final long createTime;
        
        public CacheEntry(T value) {
            this.value = value;
            this.createTime = System.currentTimeMillis();
            this.lastAccessTime = this.createTime;
        }
        
        public T getValue() {
            return value;
        }
        
        public void touch() {
            this.lastAccessTime = System.currentTimeMillis();
        }
        
        public boolean isExpired(long expireTime) {
            // 采用长时间不用按需失效策略：检查最后访问时间
            return System.currentTimeMillis() - lastAccessTime > expireTime;
        }
    }
}
