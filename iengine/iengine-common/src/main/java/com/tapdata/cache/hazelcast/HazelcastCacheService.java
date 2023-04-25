package com.tapdata.cache.hazelcast;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.tapdata.cache.AbstractCacheService;
import com.tapdata.cache.CacheUtil;
import com.tapdata.cache.ICacheGetter;
import com.tapdata.cache.ICacheStats;
import com.tapdata.cache.ICacheStore;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.mongo.ClientMongoOperator;

import java.util.Map;
import java.util.concurrent.locks.Lock;

public class HazelcastCacheService extends AbstractCacheService {

	private final static String CACHE_KEY_PREFIX = "cache-";

	private final HazelcastInstance hazelcastInstance;

	private Map<String, ICacheStats> cacheStatsMap;


	public HazelcastCacheService(HazelcastInstance hazelcastInstance, ClientMongoOperator clientMongoOperator) {
		super(clientMongoOperator);
		this.hazelcastInstance = hazelcastInstance;
		this.setCacheStatsMap(hazelcastInstance.getMap(CACHE_KEY_PREFIX + "cacheStats"));
		super.cacheStatusMap = hazelcastInstance.getMap(CACHE_KEY_PREFIX + "cacheStatus");
	}

	@Override
	public synchronized void registerCache(DataFlowCacheConfig config) {
		super.registerCache(config);
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
	protected ICacheStats getCacheStats(String cacheName) {
		return getCacheStatsMap().computeIfAbsent(cacheName, f -> new HazelcastCacheStats());
	}

	@Override
	protected ICacheStore getCacheStore(String cacheName) {
		DataFlowCacheConfig config = getConfig(cacheName);
		if (config == null) {
			return null;
		}
		return super.getCacheStoreMap().computeIfAbsent(cacheName, f -> new HazelcastCacheStore(hazelcastInstance, config));
	}

	public void setCacheStatsMap(Map<String, ICacheStats> cacheStatsMap) {
		this.cacheStatsMap = cacheStatsMap;
	}

	protected Map<String, ICacheStats> getCacheStatsMap() {
		return cacheStatsMap;
	}

}
