package com.tapdata.cache;

import java.util.List;
import java.util.Map;

/**
 * 内存缓存 - 获取接口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/9/24 上午11:50 Create
 */
public interface ICacheGetter {

  /**
   * 获取并设置缓存
   *
   * @param cacheName 缓存名
   * @param lookup    是否检查
   * @param cacheKeys 键
   * @return 集合
   * @throws InterruptedException 线程中断异常
   */
  Map<String, Object> getAndSetCache(String cacheName, Boolean lookup, Object... cacheKeys) throws Throwable;

  /**
   * 获取并设置缓存
   *
   * @param cacheName 缓存名
   * @param lookup    是否检查
   * @param cacheKeys 键
   * @return 集合
   * @throws InterruptedException 线程中断异常
   */
  List<Map<String, Object>> getAndSetCacheArray(String cacheName, Boolean lookup, Object... cacheKeys) throws Throwable;

  /**
   * 根据 缓存名+键 取缓存集合
   *
   * @param cacheName 缓存名
   * @param cacheKeys 键
   * @return 集合
   * @throws InterruptedException 线程中断异常
   */
  Map<String, Object> getCache(String cacheName, Boolean lookup, Object... cacheKeys) throws Throwable;

  /**
   * 根据 缓存名+键 取缓存集合
   *
   * @param cacheName 缓存名
   * @param cacheKeys 键
   * @return 集合
   * @throws InterruptedException 线程中断异常
   */
  default Map<String, Object> getCache(String cacheName, Object... cacheKeys) throws Throwable {
    return getCache(cacheName, true, cacheKeys);
  }

  /**
   * 获取字段值，为空返回默认值
   *
   * @param cacheName    缓存名
   * @param field        字段
   * @param defaultValue 默认值
   * @param cacheKeys    键
   * @return 字段值
   * @throws InterruptedException 线程中断异常
   */
  Object getCacheItem(String cacheName, String field, Object defaultValue, Object... cacheKeys) throws Throwable;

  /**
   * @return
   */
  default IDataSourceRowsGetter getDataSourceRowsGetter() {
    throw new RuntimeException("not support...");
  }

  default void close() {
  }
}
