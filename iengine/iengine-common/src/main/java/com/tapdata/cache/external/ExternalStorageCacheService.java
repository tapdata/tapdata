package com.tapdata.cache.external;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
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
	private final IMap<String, String> cacheGenerationMap;


	public ExternalStorageCacheService(HazelcastInstance hazelcastInstance, ClientMongoOperator clientMongoOperator) {
		super(clientMongoOperator, new ConcurrentHashMap<>());
		this.hazelcastInstance = hazelcastInstance;
		this.cacheGenerationMap = hazelcastInstance.getMap(CacheUtil.CACHE_NAME_PREFIX + "generation");
	}

	@Override
	protected String sharedCacheGeneration(String cacheName) {
		return cacheGenerationMap.get(cacheName);
	}

	@Override
	protected void publishSharedCacheGeneration(String cacheName, String generation) {
		cacheGenerationMap.put(cacheName, generation);
	}

	@Override
	protected AutoCloseable acquireSharedCacheLifecycleLock(String cacheName) {
		cacheGenerationMap.lock(cacheName);
		return () -> cacheGenerationMap.unlock(cacheName);
	}

	@Override
	protected boolean usesDistributedCacheGeneration() {
		return true;
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
	protected ICacheGetter getCacheGetterInstance(String cacheName,
			com.tapdata.entity.dataflow.DataFlowCacheConfig cacheConfig, ICacheStore cacheStore) {
		return new ExternalStorageCacheGetter(cacheStore, cacheConfig, clientMongoOperator, hazelcastInstance);
	}

	@Override
	protected ICacheStats getCacheStats(String cacheName) {
		return null;
	}

	@Override
	protected ICacheStore getCacheStore(String cacheName) {
		return getOrCreateCacheStore(cacheName, this::createCacheStore, true);
	}

	protected ICacheStore createCacheStore(com.tapdata.entity.dataflow.DataFlowCacheConfig config) {
		return new ExternalStorageCacheStore(config, hazelcastInstance);
	}
}
