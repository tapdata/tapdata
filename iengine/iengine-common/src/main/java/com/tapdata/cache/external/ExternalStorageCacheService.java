package com.tapdata.cache.external;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.cache.AbstractCacheService;
import com.tapdata.cache.CacheUtil;
import com.tapdata.cache.ICacheGetter;
import com.tapdata.cache.ICacheStats;
import com.tapdata.cache.ICacheStore;
import com.tapdata.mongo.ClientMongoOperator;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

public class ExternalStorageCacheService extends AbstractCacheService {

	private final HazelcastInstance hazelcastInstance;


	public ExternalStorageCacheService(HazelcastInstance hazelcastInstance, ClientMongoOperator clientMongoOperator) {
		super(clientMongoOperator, new ConcurrentHashMap<>());
		this.hazelcastInstance = hazelcastInstance;
	}

	@Override
	protected Lock getCacheStatusLockInstance(String cacheName) {
		return hazelcastInstance.getCPSubsystem().getLock(CacheUtil.CACHE_NAME_PREFIX + cacheName);
	}

	@Override
	protected ICacheGetter getCacheGetterInstance(String cacheName) {
		logger.info("construct a cache getter for cache [{}]", cacheName);
		com.tapdata.entity.dataflow.DataFlowCacheConfig cacheConfig = getConfig(cacheName);
		if (cacheConfig == null) {
			logger.warn("Skip constructing cache getter for [{}]: cache configuration is unavailable", cacheName);
			return null;
		}
		return new ExternalStorageCacheGetter(getCacheStore(cacheName), cacheConfig, clientMongoOperator, hazelcastInstance);
	}

	@Override
	protected ICacheStats getCacheStats(String cacheName) {
		return null;
	}

	@Override
	protected ICacheStore getCacheStore(String cacheName) {
		return super.getCacheStoreMap().computeIfAbsent(cacheName, f -> {
			com.tapdata.entity.dataflow.DataFlowCacheConfig cacheConfig = getConfig(cacheName);
			if (cacheConfig == null) {
				throw new IllegalStateException("Cache configuration is unavailable: " + cacheName);
			}
			return new ExternalStorageCacheStore(cacheConfig, hazelcastInstance);
		});
	}
}
