package com.tapdata.cache.serializer;

import com.tapdata.cache.CacheUtil;
import com.tapdata.cache.ICacheStore;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractSerializerCacheStore implements ICacheStore {

	protected final Map<String, Map<String, Object>> dataMap;
	protected final Map<String, Set<String>> indexMap;
	protected final DataFlowCacheConfig dataFlowCacheConfig;

	public AbstractSerializerCacheStore(DataFlowCacheConfig dataFlowCacheConfig, Map<String, Map<String, Object>> dataMap, Map<String, Set<String>> indexMap) {
		this.dataFlowCacheConfig = dataFlowCacheConfig;
		this.dataMap = dataMap;
		this.indexMap = indexMap;
	}

	@Override
	public void cacheRow(String cacheName, String cacheKeys, List<Map<String, Object>> rows) {

		if (CollectionUtils.isEmpty(rows)) {
			return;
		}
		List<String> primaryKeys = dataFlowCacheConfig.getPrimaryKeys();
		for (Map<String, Object> row : rows) {
			Object[] pkValues = new Object[primaryKeys.size()];
			for (int i = 0; i < primaryKeys.size(); i++) {
				pkValues[i] = MapUtil.getValueByKey(row, primaryKeys.get(i));
			}
			String pk = CacheUtil.cacheKey(cacheKeys, pkValues);
			dataMap.put(pk, row);
			Set<String> pkSet = indexMap.get(cacheKeys);
			if (CollectionUtils.isEmpty(pkSet)) {
				pkSet = new HashSet<>();
			}
			pkSet.add(pk);
			indexMap.put(cacheKeys, pkSet);
		}
	}

	@Override
	public void removeByKey(String cacheName, String cacheKey, String pkKey) {
		String pk = CacheUtil.cacheKey(cacheKey, pkKey);
		dataMap.remove(pk);
		Set<String> pkSet = indexMap.get(cacheKey);
		if (CollectionUtils.isNotEmpty(pkSet)) {
			pkSet.remove(pk);
			indexMap.put(cacheKey, pkSet);
		}

	}

	public Map<String, Map<String, Object>> getDataMap() {
		return dataMap;
	}

	public Map<String, Set<String>> getIndexMap() {
		return indexMap;
	}

	@Override
	public void destroy() {
		this.indexMap.clear();
		this.dataMap.clear();
	}
}
