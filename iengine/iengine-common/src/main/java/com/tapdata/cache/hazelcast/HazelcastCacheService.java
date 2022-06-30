package com.tapdata.cache.hazelcast;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.tapdata.cache.*;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.mongo.ClientMongoOperator;

import java.util.concurrent.locks.Lock;

public class HazelcastCacheService extends AbstractCacheService {

	private final static String CACHE_KEY_PREFIX = "cache-";

	private final HazelcastInstance hazelcastInstance;

	private ClientMongoOperator clientMongoOperator;


	public HazelcastCacheService(HazelcastInstance hazelcastInstance, ClientMongoOperator clientMongoOperator) {
		this.hazelcastInstance = hazelcastInstance;
		this.clientMongoOperator = clientMongoOperator;
		this.cacheConfigMap = hazelcastInstance.getMap(CACHE_KEY_PREFIX + "cacheConfig");
		super.setCacheStatsMap(hazelcastInstance.getMap(CACHE_KEY_PREFIX + "cacheStats"));
		super.cacheStatusMap = hazelcastInstance.getMap(CACHE_KEY_PREFIX + "cacheStatus");
	}

	@Override
	public synchronized void registerCache(DataFlowCacheConfig config) {
		super.registerCache(config);
		String cacheName = config.getCacheName();
		// 配置
		setCacheConfig(config, MemoryCacheUtil.cacheDataKey(cacheName));
		setCacheConfig(config, MemoryCacheUtil.cacheIndexKey(cacheName));
	}

	private void setCacheConfig(DataFlowCacheConfig config, String cacheConfigName) {
		MapConfig mapConfig = hazelcastInstance.getConfig().getMapConfig(cacheConfigName);
		EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
		evictionConfig.setEvictionPolicy(EvictionPolicy.LRU);
		evictionConfig.setSize(getMaxInt(config.getMaxSize()));
		mapConfig.setTimeToLiveSeconds(getMaxInt(config.getTtl()));
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
		return new HazelcastCacheGetter(getConfig(cacheName), getCacheStore(cacheName), getCacheStats(cacheName), clientMongoOperator);
	}

	@Override
	protected ICacheStats getCacheStats(String cacheName) {
		return super.getCacheStatsMap().computeIfAbsent(cacheName, f -> new HazelcastCacheStats());
	}

	@Override
	protected ICacheStore getCacheStore(String cacheName) {
		DataFlowCacheConfig config = getConfig(cacheName);
		if (config == null) {
			return null;
		}
		return super.getCacheStoreMap().computeIfAbsent(cacheName, f -> new HazelcastCacheStore(hazelcastInstance, config));
	}

}
