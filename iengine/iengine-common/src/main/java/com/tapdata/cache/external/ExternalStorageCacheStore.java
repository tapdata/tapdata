package com.tapdata.cache.external;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.cache.CacheUtil;
import com.tapdata.cache.ICacheStore;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.construct.constructImpl.BytesIMap;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import org.apache.commons.collections4.MapUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExternalStorageCacheStore implements ICacheStore {

  private final BytesIMap<Map<String, Map<String, Object>>> dataMap;
  private final DataFlowCacheConfig cacheConfig;


  public ExternalStorageCacheStore(DataFlowCacheConfig config, HazelcastInstance hazelcastInstance) {
    this.cacheConfig = config;
    ExternalStorageDto externalStorage = ExternalStorageUtil.getExternalStorage(config.getCacheNode());
    this.dataMap = new BytesIMap<>(hazelcastInstance, CacheUtil.CACHE_NAME_PREFIX + config.getCacheName(), externalStorage);
  }

  @Override
  public void cacheRow(String cacheName, String key, List<Map<String, Object>> rows) throws Throwable {

    Map<String, Map<String, Object>> recordMap;
    if (dataMap.exists(key)) {
      recordMap = dataMap.find(key);
    } else {
      recordMap = new HashMap<>(rows.size());
    }
    for (Map<String, Object> row : rows) {
      recordMap.put(CacheUtil.getPk(cacheConfig.getPrimaryKeys(), row), row);
    }
    if (MapUtils.isNotEmpty(recordMap)) {
      dataMap.insert(key, recordMap);
    }
  }

  @Override
  public void removeByKey(String cacheName, String cacheKey, String pkKey) throws Throwable {
    CacheUtil.removeRecord(dataMap, cacheKey, pkKey);
  }

  @Override
  public void destroy() {
    ICacheStore.super.destroy();
  }
}
