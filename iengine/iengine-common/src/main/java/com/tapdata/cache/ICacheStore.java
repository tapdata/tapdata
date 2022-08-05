package com.tapdata.cache;

import java.util.List;
import java.util.Map;

/**
 * cache store
 */
public interface ICacheStore {

  void cacheRow(String cacheName, String key, List<Map<String, Object>> rows);

  void removeByKey(String cacheName, String cacheKey, String pkKey);

  default void destroy() {

  }


}
