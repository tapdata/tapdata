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

  public void waitCacheNodeCompletedInitialIfNeed(String cacheName) throws InterruptedException {
    // 完成，直接返回
    if (!isInitialing.get()) return;

    // 所有cache节点正在初始化中需要等待他们初始化完成
    while (!Thread.interrupted()) {
      if (!running.get()) {
        // 完成，直接返回
        return;
      }

      List<StageRuntimeStats> stageRuntimeStats = cacheStageRuntimeStats.getCacheStageRuntimeStats(null, cacheName);
      if (null != stageRuntimeStats && !stageRuntimeStats.isEmpty()) {
        if (!stageRuntimeStats.stream().allMatch(cacheStageRuntimeStat -> {
          String status = cacheStageRuntimeStat.getStatus();
          return !StringUtils.isBlank(status) && !ConnectorConstant.STATS_STATUS_INITIALIZING.equals(status);
        })) {
          logger.warn("Waiting all cache node complete initial: " + cacheName);
          Thread.sleep(3000L);
          continue;
        }
      }

      logger.info("All cache node completed initial sync: " + cacheName);
      isInitialing.set(false);

      break;
    }
  }

}
