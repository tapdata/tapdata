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
import java.util.concurrent.TimeUnit;

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
		try {
			if (!cacheGenerationMap.tryLock(cacheName, 5, TimeUnit.SECONDS)) {
				throw new IllegalStateException("Timed out acquiring cache lifecycle lock: " + cacheName);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted acquiring cache lifecycle lock: " + cacheName, e);
		}
		return () -> cacheGenerationMap.unlock(cacheName);
	}

	@Override
	protected void finishSharedCacheDestroy(String cacheName, String generation) {
		cacheGenerationMap.remove(cacheName, generation);
	}

	@Override
	protected void publishSharedCacheDestroyFence(String cacheName, String generation) {
		cacheGenerationMap.set(cacheName, generation, CACHE_DESTROY_FENCE_LEASE_SECONDS, TimeUnit.SECONDS);
	}

	@Override
	protected void renewSharedCacheDestroyFence(String cacheName, String generation) {
		boolean locked = false;
		try {
			locked = cacheGenerationMap.tryLock(cacheName, 1, TimeUnit.SECONDS);
			if (locked && generation.equals(cacheGenerationMap.get(cacheName))) {
				cacheGenerationMap.set(cacheName, generation, CACHE_DESTROY_FENCE_LEASE_SECONDS, TimeUnit.SECONDS);
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} finally {
			if (locked) cacheGenerationMap.unlock(cacheName);
		}
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
