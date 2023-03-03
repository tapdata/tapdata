package com.tapdata.cache.external;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.cache.*;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.construct.constructImpl.ConstructIMap;
import io.tapdata.construct.constructImpl.DocumentIMap;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ExternalStorageCacheGetter extends AbstractCacheGetter {


  private final DataFlowCacheConfig cacheConfig;
  private final ClientMongoOperator clientMongoOperator;
  private final HazelcastInstance hazelcastInstance;

  private final ExternalStorageDto externalStorageDto;

  private final ConstructIMap<Map<String, Map<String, Object>>> dataMap;


  public ExternalStorageCacheGetter(ICacheStore cacheStore, DataFlowCacheConfig config, ClientMongoOperator clientMongoOperator, HazelcastInstance hazelcastInstance) {

    super(config, cacheStore, new ExternalStorageCacheStats(), clientMongoOperator);

    this.cacheConfig = config;
    this.clientMongoOperator = clientMongoOperator;
    this.hazelcastInstance = hazelcastInstance;
    String cacheName = this.cacheConfig.getCacheName();
    this.externalStorageDto = ExternalStorageUtil.getExternalStorage(cacheConfig.getCacheNode());
    this.dataMap = new DocumentIMap<>(hazelcastInstance, CacheUtil.CACHE_NAME_PREFIX + cacheName, externalStorageDto);
  }


  @Override
  protected List<Map<String, Object>> getRecordList(String cacheName, Object... cacheKeys) throws Throwable {
    Map<String, Map<String, Object>> recordMap = this.dataMap.find(CacheUtil.cacheKey(cacheKeys));
    List<Map<String, Object>> resultList = new ArrayList<>();
    if (recordMap != null && CollectionUtils.isNotEmpty(recordMap.values())) {
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
    if (recordMap != null && CollectionUtils.isNotEmpty(recordMap.values())) {
      Optional<Map<String, Object>> optional = recordMap.values().stream().findFirst();
      if (optional.isPresent()) {
        result = CacheUtil.returnCacheRow(optional.get());
      }
    }
    return result;
  }

  @Override
  public IDataSourceRowsGetter getDataSourceRowsGetter() {
    if (dataSourceRowsGetter == null) {
      logger.info("construct a data source rows getter for cache [{}]", cacheConfig.getCacheName());
      this.dataSourceRowsGetter = new PdkDataSourceRowsGetter(cacheConfig, clientMongoOperator, hazelcastInstance);
    }
    return dataSourceRowsGetter;
  }


}
