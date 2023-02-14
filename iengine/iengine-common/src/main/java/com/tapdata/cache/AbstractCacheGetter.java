package com.tapdata.cache;

import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.exception.DataFlowException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractCacheGetter implements ICacheGetter {

  protected final Logger logger = LogManager.getLogger(AbstractCacheGetter.class);

  protected final DataFlowCacheConfig cacheConfig;

  protected final ICacheStats cacheStats;

  protected final ClientMongoOperator clientMongoOperator;

  protected IDataSourceRowsGetter dataSourceRowsGetter;

  private final ICacheStore cacheStore;

  public AbstractCacheGetter(DataFlowCacheConfig cacheConfig, ICacheStore cacheStore, ICacheStats cacheStats, ClientMongoOperator clientMongoOperator) {
    this.cacheConfig = cacheConfig;
    this.cacheStats = cacheStats;
    this.cacheStore = cacheStore;
    this.clientMongoOperator = clientMongoOperator;
  }

  abstract protected List<Map<String, Object>> getRecordList(String cacheName, Object... cacheKeys) throws Throwable;

  abstract protected Map<String, Object> getRecord(String cacheName, Object... cacheKeys) throws Throwable;

  @Override
  public Map<String, Object> getCache(String cacheName, Boolean lookup, Object... cacheKeys) throws Throwable {
    Map<String, Object> result = getRecord(cacheName, cacheKeys);
    if (result == null && lookup) {
      result = getAndSetCache(cacheName, true, cacheKeys);
      cacheStats.hitRateStats(cacheName, false);
    } else {
      cacheStats.hitRateStats(cacheName, true);
    }
    return result;
  }

  @Override
  public Object getCacheItem(String cacheName, String field, Object defaultValue, Object... cacheKeys) throws Throwable {
    Map<String, Object> row = getAndSetCache(cacheName, true, cacheKeys);

    return row == null ? defaultValue : row.getOrDefault(field, defaultValue);
  }

  @Override
  public Map<String, Object> getAndSetCache(String cacheName, Boolean lookup, Object... cacheKeys) throws Throwable {
    Map<String, Object> record = getRecord(cacheName, cacheKeys);
    if (record == null && lookup) {
      List<Map<String, Object>> rows = getDataSourceRowsGetter().getRows(cacheKeys);
      if (CollectionUtils.isNotEmpty(rows)) {
        String key = CacheUtil.cacheKey(cacheKeys);
        cacheStore.cacheRow(cacheName, key, rows);
        record = CacheUtil.returnCacheRow(rows.get(rows.size() - 1));
        cacheStats.hitRateStats(cacheName, false);
      }
    } else {
      cacheStats.hitRateStats(cacheName, true);
    }
    return record;
  }

  @Override
  public List<Map<String, Object>> getAndSetCacheArray(String cacheName, Boolean lookup, Object... cacheKeys) throws Throwable {
    List<Map<String, Object>> recordList = getRecordList(cacheName, cacheKeys);
    if (CollectionUtils.isEmpty(recordList) && lookup) {
      IDataSourceRowsGetter iDataSourceRowsGetter = getDataSourceRowsGetter();
      List<Map<String, Object>> rows = iDataSourceRowsGetter.getRows(cacheKeys);
      if (CollectionUtils.isNotEmpty(rows)) {
        recordList = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
          recordList.add(CacheUtil.returnCacheRow(row));
        }
        String key = CacheUtil.cacheKey(cacheKeys);
        cacheStore.cacheRow(cacheName, key, rows);
        cacheStats.hitRateStats(cacheName, false);
      }
    } else {
      cacheStats.hitRateStats(cacheName, true);
    }
    return recordList;
  }


  @Override
  public IDataSourceRowsGetter getDataSourceRowsGetter() {
    if (dataSourceRowsGetter == null) {
      Connections sourceConnection = getSourceConnection(cacheConfig);
      try {
        DatabaseTypeEnum databaseTypeEnum = DatabaseTypeEnum.fromString(sourceConnection.getDatabase_type());
        if (databaseTypeEnum == DatabaseTypeEnum.MONGODB || databaseTypeEnum == DatabaseTypeEnum.ALIYUN_MONGODB) {
          this.dataSourceRowsGetter = new MongoRowsGetter(cacheConfig);
        } else {
          throw new DataFlowException("Cache with database type " + databaseTypeEnum.getName() + " is not supported ");
        }
      } catch (Exception e) {
        logger.error(
                "Get datasource rows getter failed,  connection {} database type {} reason {}, will turn off lookup mode.",
                sourceConnection.getName(),
                sourceConnection.getDatabase_type(),
                e.getMessage(),
                e
        );
      }
    }
    return dataSourceRowsGetter;
  }

  /**
   * 获取源
   *
   * @return
   */
  protected Connections getSourceConnection(DataFlowCacheConfig cacheConfig) {
    return cacheConfig.getSourceConnection();
  }

  @Override
  public void close() {
    if (dataSourceRowsGetter != null) {
      dataSourceRowsGetter.close();
    }
  }
}
