package com.tapdata.tm.task.utils;

import com.alibaba.fastjson.JSON;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class CacheUtils {

    /**
     * 初始化缓存
     */
    private static final LoadingCache<String, Object> CACHE = CacheBuilder.newBuilder()
            //最大容量为100（基于容量进行回收）
            .maximumSize(100)
            //配置写入后多久使缓存过期-下文会讲述
            .expireAfterWrite(150, TimeUnit.SECONDS)
            //配置写入后多久刷新缓存-下文会讲述
            .refreshAfterWrite(1, TimeUnit.SECONDS)
            //key使用弱引用-WeakReference
            .weakKeys()
            //当Entry被移除时的监听器
            .removalListener(notification -> log.info("notification={}", JSON.toJSON(notification)))
            //创建一个CacheLoader，重写load方法，以实现"当get时缓存不存在，则load，放到缓存，并返回"的效果
            .build(new CacheLoader<String, Object>() {
                //重点，自动写缓存数据的方法，必须要实现
                @Override
                public String load(String key) throws Exception {
                    return null;
                }
                //异步刷新缓存-下文会讲述
                @Override
                public ListenableFuture<Object> reload(String key, Object oldValue) throws Exception {
                    return super.reload(key, oldValue);
                }
            });

    /**
     * 存入缓存
     *
     * @param key
     * @param value
     */
    public static void put(String key, Object value) {
        try {
            CACHE.put(key, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取缓存
     *
     * @param key
     */
    public static Object get(String key) {
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
    public static boolean isExist(String key) {

        return CACHE.asMap().containsKey(key);
    }

    public static Object invalidate(String key) {
        try {
            return CACHE.get(key);
        } catch (Exception e) {
            return null;
        } finally {
            CACHE.invalidate(key);
        }
    }

    public static void getAll() {
        try {
            CACHE.getAll(CACHE.asMap().keySet());
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}
