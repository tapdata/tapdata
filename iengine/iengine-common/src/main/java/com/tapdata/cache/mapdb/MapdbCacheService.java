package com.tapdata.cache.mapdb;

import com.tapdata.cache.AbstractCacheService;
import com.tapdata.cache.ICacheGetter;
import com.tapdata.cache.ICacheStats;
import com.tapdata.cache.ICacheStore;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.mongo.ClientMongoOperator;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MapdbCacheService extends AbstractCacheService {

  private final Map<String, DB> dbMap;

  private Map<String, ICacheStats> cacheStatsMap;


  public MapdbCacheService(ClientMongoOperator clientMongoOperator) {
    super(clientMongoOperator);
    this.dbMap = new ConcurrentHashMap<>();
    super.cacheStatusMap = new ConcurrentHashMap<>();
  }

  @Override
  protected Lock getCacheStatusLockInstance(String cacheName) {
    return new ReentrantLock();
  }

  private DB getDB(String cacheName) {
//    String tapdataWorkDir = System.getenv("TAPDATA_WORK_DIR");
//    Path cachePath = Paths.get(tapdataWorkDir, "cache", cacheName);
//    try {
//      if (Files.notExists(cachePath.getParent())) {
//        logger.info("create dir --> {}", cachePath.getParent());
//        Files.createDirectories(cachePath.getParent());
//      }
//      Files.deleteIfExists(cachePath);
//    } catch (IOException e) {
//      logger.error("delete cache file error", e);
//    }

//    return DBMaker.fileDB(cachePath.toFile())
//      .fileMmapEnable()
////      .fileMmapPreclearDisable()
////      .allocateStartSize(15 * 1024 * 1024 * 1024)  // 1GB
////      .allocateIncrement(512 * 1024 * 1024)
//      .fileDeleteAfterClose()
////      .cleanerHackEnable()
//      .make();
    return dbMap.computeIfAbsent(cacheName, f -> DBMaker.memoryDB().executorEnable().closeOnJvmShutdown().make());
  }

  @Override
  public void registerCache(DataFlowCacheConfig cacheConfig) {
    super.registerCache(cacheConfig);
    String cacheName = cacheConfig.getCacheName();
    this.dbMap.put(cacheName, getDB(cacheName));
  }

  @Override
  protected ICacheGetter getCacheGetterInstance(String cacheName) {
    return new MapdbCacheGetter(getConfig(cacheName), getCacheStore(cacheName), getCacheStats(cacheName), null);
  }

  @Override
  protected ICacheStats getCacheStats(String cacheName) {
    return getCacheStatsMap().computeIfAbsent(cacheName, f -> new MapdbCacheStats());
  }

  @Override
  protected ICacheStore getCacheStore(String cacheName) {
    return super.getCacheStoreMap().computeIfAbsent(cacheName, f -> new MapdbCacheStore(getConfig(cacheName), dbMap.get(cacheName)));
  }

  public void setCacheStatsMap(Map<String, ICacheStats> cacheStatsMap) {
    this.cacheStatsMap = cacheStatsMap;
  }

  protected Map<String, ICacheStats> getCacheStatsMap() {
    return cacheStatsMap;
  }

}
