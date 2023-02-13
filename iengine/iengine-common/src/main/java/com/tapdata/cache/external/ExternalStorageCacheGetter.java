package com.tapdata.cache.external;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.cache.AbstractCacheGetter;
import com.tapdata.cache.CacheUtil;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.construct.constructImpl.BytesIMap;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ExternalStorageCacheGetter extends AbstractCacheGetter {


  private DataFlowCacheConfig cacheConfig;
  private ClientMongoOperator clientMongoOperator;
  private HazelcastInstance hazelcastInstance;

  private ExternalStorageDto externalStorageDto;

  private final BytesIMap<Map<String, Map<String, Object>>> dataMap;


  public ExternalStorageCacheGetter(DataFlowCacheConfig config, ClientMongoOperator clientMongoOperator, HazelcastInstance hazelcastInstance) {

    super(config, null, null, clientMongoOperator);

    this.cacheConfig = config;
    this.clientMongoOperator = clientMongoOperator;
    this.hazelcastInstance = hazelcastInstance;
    String cacheName = this.cacheConfig.getCacheName();
    this.externalStorageDto = ExternalStorageUtil.getExternalStorage(cacheConfig.getCacheNode());
    this.dataMap = new BytesIMap<>(hazelcastInstance, CacheUtil.CACHE_NAME_PREFIX + cacheName, externalStorageDto);
  }


  @Override
  protected List<Map<String, Object>> getRecordList(String cacheName, Object... cacheKeys) throws Throwable {
    Map<String, Map<String, Object>> recordMap = this.dataMap.find(CacheUtil.cacheKey(cacheKeys));
    List<Map<String, Object>> resultList = new ArrayList<>();
    if (CollectionUtils.isNotEmpty(recordMap.values())) {
      for (Map<String, Object> map : recordMap.values()) {
        resultList.add(CacheUtil.returnCacheRow(map));
      }
    }
    return resultList;
  }

  @Override
  protected Map<String, Object> getRecord(String cacheName, Object... cacheKeys) throws Throwable {
    Map<String, Map<String, Object>> recordMap = this.dataMap.find(CacheUtil.cacheKey(cacheKeys));
    Map<String, Object> result = null;
    if (CollectionUtils.isNotEmpty(recordMap.values())) {
      Optional<Map<String, Object>> optional = recordMap.values().stream().findFirst();
      if (optional.isPresent()) {
        result = CacheUtil.returnCacheRow(optional.get());
      }
    }
    return result;
  }


}
