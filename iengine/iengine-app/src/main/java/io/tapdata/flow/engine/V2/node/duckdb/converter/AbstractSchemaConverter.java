package io.tapdata.flow.engine.V2.node.duckdb.converter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 抽象的 Schema 转换器基类
 * 提供缓存支持和通用功能
 * 
 * @param <S> 源类型
 * @param <T> 目标类型
 */
public abstract class AbstractSchemaConverter<S, T> implements SchemaConverter<S, T> {
    
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    
    /**
     * 缓存，键为源对象的唯一标识，值为目标对象
     */
    protected final Map<String, CacheEntry<T>> cache = new ConcurrentHashMap<>();
    
    /**
     * 缓存过期时间（毫秒），永不清理
     */
    protected long cacheExpireTime = TimeUnit.DAYS.toMillis(36500);
    
    /**
     * 上次清理时间
     */
    protected volatile long lastCleanupTime = System.currentTimeMillis();
    
    /**
     * 清理间隔（毫秒），默认 永不清理
     */
    protected long cleanupInterval = TimeUnit.DAYS.toMillis(36500);
    
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
     * 获取源对象的唯一标识
     * 
     * @param source 源对象
     * @return 唯一标识
     */
    protected abstract String getSourceKey(S source);
    
    /**
     * 执行实际的转换逻辑
     * 
     * @param source 源对象
     * @return 目标对象
     */
    protected abstract T doConvert(S source);
    
    @Override
    public T convert(S source) {
        if (source == null) {
            return null;
        }
        
        // 尝试从缓存获取
        String key = getSourceKey(source);
        CacheEntry<T> entry = cache.get(key);
        
        // 检查缓存是否有效
        if (entry != null && !entry.isExpired(cacheExpireTime)) {
            entry.touch(); // 更新访问时间
            logger.debug("Cache hit for key: {}", key);
            return entry.getValue();
        }
        
        // 缓存未命中或已过期，执行转换
        logger.debug("Cache miss for key: {}, converting...", key);
        T result = doConvert(source);
        
        // 缓存结果
        if (result != null) {
            cache.put(key, new CacheEntry<>(result));
        }
        
        // 尝试清理过期缓存
        tryCleanup();
        
        return result;
    }
    
    @Override
    public boolean isCached(S source) {
        if (source == null) {
            return false;
        }
        
        String key = getSourceKey(source);
        CacheEntry<T> entry = cache.get(key);
        return entry != null && !entry.isExpired(cacheExpireTime);
    }
    
    @Override
    public void cache(S source, T target) {
        if (source == null || target == null) {
            return;
        }
        
        String key = getSourceKey(source);
        cache.put(key, new CacheEntry<>(target));
        logger.debug("Cached result for key: {}", key);
    }
    
    @Override
    public void clearCache() {
        cache.clear();
        logger.debug("Cache cleared");
    }
    
    /**
     * 尝试清理过期缓存
     */
    protected void tryCleanup() {
        long now = System.currentTimeMillis();
        if (now - lastCleanupTime < cleanupInterval) {
            return;
        }
        
        lastCleanupTime = now;
        
        // 清理过期条目
        int removedCount = 0;
        for (Map.Entry<String, CacheEntry<T>> entry : cache.entrySet()) {
            if (entry.getValue().isExpired(cacheExpireTime)) {
                cache.remove(entry.getKey());
                removedCount++;
            }
        }
        
        if (removedCount > 0) {
            logger.debug("Cleaned up {} expired cache entries", removedCount);
        }
    }
    
    /**
     * 缓存条目
     */
    protected static class CacheEntry<T> {
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
