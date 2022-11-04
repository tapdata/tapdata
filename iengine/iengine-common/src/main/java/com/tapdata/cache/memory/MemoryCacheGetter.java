package com.tapdata.cache.memory;

import com.tapdata.cache.AbstractCacheGetter;
import com.tapdata.cache.ICacheRuntimeStats;
import com.tapdata.cache.ICacheStats;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.entity.dataflow.StageRuntimeStats;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class MemoryCacheGetter extends AbstractCacheGetter {

  /**
   * - key: cache key
   * - value:
   * -- key: primary keys
   * -- value: record
   */
  private final Map<String, Map<String, Map<String, Object>>> cacheData;

  private final ICacheRuntimeStats cacheStageRuntimeStats;

  private final AtomicBoolean isInitialing;

  private final AtomicBoolean running;

  private final long lastLogTS;


  public MemoryCacheGetter(DataFlowCacheConfig cacheConfig,
                           Map<String, Map<String, Map<String, Object>>> cacheData,
                           ICacheStats cacheStats,
                           ICacheRuntimeStats cacheStageRuntimeStats, ClientMongoOperator clientMongoOperator) {
    super(cacheConfig, new MemoryCacheStore(cacheData, cacheStats, cacheConfig), cacheStats, clientMongoOperator);
    this.cacheData = cacheData;
    this.isInitialing = new AtomicBoolean(true);
    this.running = new AtomicBoolean(true);
    this.lastLogTS = 0L;
    this.cacheStageRuntimeStats = cacheStageRuntimeStats;
  }

  @Override
  protected List<Map<String, Object>> getRecordList(String cacheName, Object... cacheKeys) throws InterruptedException {
    return null;
  }

  @Override
  protected Map<String, Object> getRecord(String cacheName, Object... cacheKeys) throws InterruptedException {
    return null;
  }

}
