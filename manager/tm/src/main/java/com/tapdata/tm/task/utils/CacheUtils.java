package com.tapdata.tm.task.utils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.tapdata.tm.utils.Lists;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;

import javax.swing.*;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CacheUtils {

    /**
     * 初始化缓存
     */
    private static final LoadingCache<Object, Object> CACHE = CacheBuilder.newBuilder()
            // 缓存池大小，在缓存项接近该大小时， Guava开始回收旧的缓存项
            .maximumSize(16)
            // 设置缓存在写入之后在设定时间后失效
            .expireAfterWrite(12, TimeUnit.HOURS)
            .build(new CacheLoader<Object, Object>() {
                @Override
                public Object load(@NotNull Object key) {
                    // 处理缓存键不存在缓存值时的处理逻辑
                    return null;
                }
            });

    /**
     * 存入缓存
     *
     * @param key
     * @param value
     */
    public static void put(Object key, Object value) {
        CACHE.put(key, value);
    }

    @SneakyThrows
    public static void push(Object key, Object value) {
        if (isExist(key)) {
            List<Object> data = (List<Object>) CACHE.get(key);
            data.add(value);
            CACHE.put(key, data);
        } else {
            CACHE.put(key, Lists.newArrayList(value));
        }
    }

    /**
     * 获取缓存
     *
     * @param key
     */
    public static Object get(Object key) {
        try {
            return CACHE.get(key);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 是否存在key
     *
     * @param key
     * @return 是否存在key
     */
    public static boolean isExist(Object key) {
        return CACHE.asMap().containsKey(key);
    }

    public static Object invalidate(Object key) {
        try {
            return CACHE.get(key);
        } catch (Exception e) {
            return null;
        } finally {
            CACHE.invalidate(key);
        }
    }

}
