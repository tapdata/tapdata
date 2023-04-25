package com.tapdata.cache.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.tapdata.cache.CacheUtil;
import com.tapdata.cache.serializer.AbstractSerializerCacheStore;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;

import java.util.Map;
import java.util.Set;

public class HazelcastCacheStore extends AbstractSerializerCacheStore {

	private final HazelcastInstance hazelcastInstance;

	private Map<String, Map<String, Integer>> stageFieldProjection;


	public HazelcastCacheStore(HazelcastInstance hazelcastInstance, DataFlowCacheConfig cacheConfig) {
		super(cacheConfig,
				hazelcastInstance.getMap(CacheUtil.cacheDataKey(cacheConfig.getCacheName())),
				hazelcastInstance.getMap(CacheUtil.cacheIndexKey(cacheConfig.getCacheName())));
		this.hazelcastInstance = hazelcastInstance;
	}


	@Override
	public void destroy() {
		if (this.indexMap instanceof IMap) {
			((IMap<String, Set<String>>) this.indexMap).destroy();
		}
		if (this.dataMap instanceof IMap) {
			((IMap<String, Map<String, Object>>) this.dataMap).destroy();
		}
	}
}
