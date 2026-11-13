package com.tapdata.cache.hazelcast;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.tapdata.cache.AbstractCacheService;
import com.tapdata.cache.CacheUtil;
import com.tapdata.cache.ICacheGetter;
import com.tapdata.cache.ICacheStats;
import com.tapdata.cache.ICacheStore;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.mongo.ClientMongoOperator;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public class HazelcastCacheService extends AbstractCacheService {

	private final static String CACHE_KEY_PREFIX = "cache-";

	private final HazelcastInstance hazelcastInstance;
	private final IMap<String, String> cacheGenerationMap;

	private Map<String, ICacheStats> cacheStatsMap;


	public HazelcastCacheService(HazelcastInstance hazelcastInstance, ClientMongoOperator clientMongoOperator) {
		super(clientMongoOperator);
		this.hazelcastInstance = hazelcastInstance;
		this.cacheGenerationMap = hazelcastInstance.getMap(CacheUtil.CACHE_NAME_PREFIX + "generation");
		this.setCacheStatsMap(hazelcastInstance.getMap(CACHE_KEY_PREFIX + "cacheStats"));
		super.cacheStatusMap = hazelcastInstance.getMap(CACHE_KEY_PREFIX + "cacheStatus");
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
	// Base registration and refresh hold the same per-cache lifecycle lock as destroy.
	protected void onCacheRegistered(DataFlowCacheConfig config) {
		super.onCacheRegistered(config);
		String cacheName = config.getCacheName();
		// 配置
		setCacheConfig(config, CacheUtil.cacheDataKey(cacheName));
		setCacheConfig(config, CacheUtil.cacheIndexKey(cacheName));
	}

	private void setCacheConfig(DataFlowCacheConfig config, String cacheConfigName) {
		MapConfig mapConfig = hazelcastInstance.getConfig().getMapConfig(cacheConfigName);
		EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
		evictionConfig.setEvictionPolicy(EvictionPolicy.LRU);
		evictionConfig.setMaxSizePolicy(MaxSizePolicy.FREE_HEAP_SIZE);
		evictionConfig.setSize(getMaxInt(config.getMaxSize()));
	}


	private int getMaxInt(long maxSize) {
		if (maxSize > Integer.MAX_VALUE) {
			maxSize = Integer.MAX_VALUE;
		}
		return (int) maxSize;
	}


	@Override
	protected Lock getCacheStatusLockInstance(String cacheName) {
		return hazelcastInstance.getCPSubsystem().getLock(CACHE_KEY_PREFIX + cacheName);
	}

	@Override
	protected ICacheGetter getCacheGetterInstance(String cacheName) {
		logger.info("construct a cache getter for cache [{}]", cacheName);
		return new HazelcastCacheGetter(getConfig(cacheName), getCacheStore(cacheName), getCacheStats(cacheName), clientMongoOperator, hazelcastInstance);
	}

	@Override
	protected ICacheGetter getCacheGetterInstance(String cacheName, DataFlowCacheConfig config, ICacheStore store) {
		logger.info("construct a cache getter for cache [{}]", cacheName);
		return new HazelcastCacheGetter(config, store, getCacheStats(cacheName), clientMongoOperator, hazelcastInstance);
	}

	@Override
	protected ICacheStats getCacheStats(String cacheName) {
		return getCacheStatsMap().computeIfAbsent(cacheName, f -> new HazelcastCacheStats());
	}

	@Override
	protected ICacheStore getCacheStore(String cacheName) {
		return getOrCreateCacheStore(cacheName, config -> new HazelcastCacheStore(hazelcastInstance, config), false);
	}

	public void setCacheStatsMap(Map<String, ICacheStats> cacheStatsMap) {
		this.cacheStatsMap = cacheStatsMap;
	}

	protected Map<String, ICacheStats> getCacheStatsMap() {
		return cacheStatsMap;
	}

}
