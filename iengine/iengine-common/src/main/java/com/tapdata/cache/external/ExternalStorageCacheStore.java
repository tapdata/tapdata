package com.tapdata.cache.external;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.cache.CacheUtil;
import com.tapdata.cache.ICacheStore;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import io.tapdata.construct.constructImpl.ConstructIMap;
import io.tapdata.construct.constructImpl.DocumentIMap;
import io.tapdata.flow.engine.V2.util.ExternalStorageUtil;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExternalStorageCacheStore implements ICacheStore {
	protected final Logger logger = LogManager.getLogger(ExternalStorageCacheStore.class);

	private final String referenceId;
	private final ConstructIMap<Map<String, Map<String, Object>>> dataMap;
	private final DataFlowCacheConfig cacheConfig;


	public ExternalStorageCacheStore(DataFlowCacheConfig config, HazelcastInstance hazelcastInstance) {
		this.cacheConfig = config;
		ExternalStorageDto externalStorage = ExternalStorageUtil.getExternalStorage(config.getCacheNode());
		this.referenceId = String.format("%s", getClass().getSimpleName());
		this.dataMap = new DocumentIMap<>(hazelcastInstance, referenceId, CacheUtil.CACHE_NAME_PREFIX + config.getCacheName(), externalStorage);
	}

	@Override
	public void cacheRow(String cacheName, String key, List<Map<String, Object>> rows) throws Throwable {

		Map<String, Map<String, Object>> recordMap;
		if (dataMap.exists(key)) {
			recordMap = dataMap.find(key);
		} else {
			recordMap = new HashMap<>(rows.size());
		}
		for (Map<String, Object> row : rows) {
			recordMap.put(CacheUtil.getPk(cacheConfig.getPrimaryKeys(), row), row);
		}
		if (MapUtils.isNotEmpty(recordMap)) {
			dataMap.insert(key, recordMap);
		}
	}

	@Override
	public void removeByKey(String cacheName, String cacheKey, String pkKey) throws Throwable {
		CacheUtil.removeRecord(dataMap, cacheKey, pkKey);
	}

	@Override
	public void destroy() {
		try {
			dataMap.destroy();
		} catch (Exception e) {
			logger.warn("Destroy cache failed: {}", e.getMessage());
		}
		ICacheStore.super.destroy();
	}
}
