package com.tapdata.cache.external;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.cache.*;
import com.tapdata.cache.hazelcast.HazelcastCacheGetter;
import com.tapdata.mongo.ClientMongoOperator;

import java.util.concurrent.locks.Lock;

public class ExternalStorageCacheService extends AbstractCacheService {

  private final HazelcastInstance hazelcastInstance;


  public ExternalStorageCacheService(ClientMongoOperator clientMongoOperator, HazelcastInstance hazelcastInstance) {
    super(clientMongoOperator);
    this.hazelcastInstance = hazelcastInstance;
  }

  @Override
  protected Lock getCacheStatusLockInstance(String cacheName) {
    return hazelcastInstance.getCPSubsystem().getLock(CacheUtil.CACHE_NAME_PREFIX + cacheName);
  }

  @Override
  protected ICacheGetter getCacheGetterInstance(String cacheName) {
    logger.info("construct a cache getter for cache [{}]", cacheName);
    return new ExternalStorageCacheGetter(getConfig(cacheName), clientMongoOperator, hazelcastInstance);  }

  @Override
  protected ICacheStats getCacheStats(String cacheName) {
    return null;
  }

  @Override
  protected ICacheStore getCacheStore(String cacheName) {
    return null;
  }
}
