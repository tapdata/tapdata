package com.tapdata.tm.commons.dag.process.converter;

import com.tapdata.tm.commons.dag.process.dto.TapFieldDto;
import com.tapdata.tm.commons.dag.process.dto.TapTableDto;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.commons.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * TM 端 Schema 转换器
 * 将 Schema 列表转换为 TapTableDto 列表
 * 支持缓存和类型预计算
 */
public class TmSchemaConverter {
    
    private static final Logger logger = LoggerFactory.getLogger(TmSchemaConverter.class);
    
    /**
     * 缓存，键为 Schema 标识（nodeId + name），值为 TapTableDto
     */
    private final Map<String, CacheEntry<TapTableDto>> cache = new ConcurrentHashMap<>();
    
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
     * 将 Schema 列表转换为 TapTableDto 列表
     * 
     * @param schemas Schema 列表
     * @return TapTableDto 列表
     */
    public List<TapTableDto> convert(List<Schema> schemas) {
        if (schemas == null || schemas.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<TapTableDto> result = new ArrayList<>();
        for (Schema schema : schemas) {
            TapTableDto dto = convertSingle(schema);
            if (dto != null) {
                result.add(dto);
            }
        }
        
        return result;
    }
    
    /**
     * 转换单个 Schema
     * 
     * @param schema Schema 对象
     * @return TapTableDto
     */
    public TapTableDto convertSingle(Schema schema) {
        if (schema == null) {
            return null;
        }
        
        // 尝试从缓存获取
        String key = getSchemaKey(schema);
        CacheEntry<TapTableDto> entry = cache.get(key);
        
        // 检查缓存是否有效
        if (entry != null && !entry.isExpired(cacheExpireTime)) {
            entry.touch();
            logger.debug("Cache hit for schema: {}", key);
            return entry.getValue();
        }
        
        // 缓存未命中，执行转换
        logger.debug("Cache miss for schema: {}, converting...", key);
        TapTableDto dto = doConvert(schema);
        
        // 缓存结果
        if (dto != null) {
            cache.put(key, new CacheEntry<>(dto));
        }
        
        // 尝试清理过期缓存
        tryCleanup();
        
        return dto;
    }
    
    /**
     * 实际转换逻辑
     */
    private TapTableDto doConvert(Schema schema) {
        TapTableDto dto = new TapTableDto();
        
        // 设置 ID
        Object nodeId = schema.getNodeId();
        String nodeIdStr;
        if (nodeId != null) {
            nodeIdStr = nodeId instanceof org.bson.types.ObjectId ? 
                ((org.bson.types.ObjectId) nodeId).toString() : 
                nodeId.toString();
        } else {
            Object id = schema.getId();
            nodeIdStr = id != null ? id.toString() : null;
        }
        dto.setId(nodeIdStr);
        
        // 设置名称
        dto.setName(schema.getName());
        
        // 转换字段和主键
        List<String> primaryKeys = new ArrayList<>();
        List<TapFieldDto> fieldDtos = new ArrayList<>();
        
        if (schema.getFields() != null) {
            for (Field field : schema.getFields()) {
                if (Boolean.TRUE.equals(field.getPrimaryKey())) {
                    primaryKeys.add(field.getFieldName());
                }
                
                TapFieldDto fieldDto = convertField(field);
                if (fieldDto != null) {
                    fieldDtos.add(fieldDto);
                }
            }
        }
        
        dto.setPrimaryKeys(primaryKeys);
        dto.setFields(fieldDtos);
        
        logger.debug("Converted schema: {} to TapTableDto with {} fields", 
            schema.getName(), fieldDtos.size());
        
        return dto;
    }
    
    /**
     * 转换单个 Field
     */
    private TapFieldDto convertField(Field field) {
        if (field == null) {
            return null;
        }
        
        TapFieldDto dto = new TapFieldDto();
        dto.setName(field.getFieldName());
        dto.setOriginalFieldName(field.getOriginalFieldName());
        dto.setDataType(field.getDataType());
        dto.setIsPrimaryKey(Boolean.TRUE.equals(field.getPrimaryKey()));
        
        if (field.getPrimaryKeyPosition() != null && field.getPrimaryKeyPosition() > 0) {
            dto.setPrimaryKeyPos(field.getPrimaryKeyPosition());
        }
        
        dto.setNullable(field.getIsNullable() != null && field.getIsNullable() instanceof Boolean b ? b : true);
        
        if (field.getColumnPosition() != null && field.getColumnPosition() > 0) {
            dto.setPos(field.getColumnPosition());
        }
        
        // 设置 TapType 信息
        if (field.getTapType() != null) {
            dto.setTapTypeName(field.getTapType().getClass().getSimpleName());
        }
        
        // 预计算 Arrow 和 DuckDB 类型
        if (field.getDataType() != null) {
            dto.precomputeTypes(field.getDataType());
        }
        
        return dto;
    }
    
    /**
     * 获取 Schema 的唯一标识
     */
    private String getSchemaKey(Schema schema) {
        Object nodeId = schema.getNodeId();
        String idStr = nodeId != null ? nodeId.toString() : 
            (schema.getId() != null ? schema.getId().toString() : "unknown");
        return idStr + "_" + schema.getName();
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
        
        int removedCount = 0;
        for (Map.Entry<String, CacheEntry<TapTableDto>> entry : cache.entrySet()) {
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
     * 清空缓存
     */
    public void clearCache() {
        cache.clear();
        logger.debug("Cache cleared");
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
