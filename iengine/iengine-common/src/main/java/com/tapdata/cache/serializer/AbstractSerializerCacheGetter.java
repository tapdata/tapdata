package com.tapdata.cache.serializer;

import com.tapdata.cache.AbstractCacheGetter;
import com.tapdata.cache.CacheUtil;
import com.tapdata.cache.ICacheStats;
import com.tapdata.cache.ICacheStore;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractSerializerCacheGetter extends AbstractCacheGetter {

	private final Map<String, Set<String>> indexMap;
	private final Map<String, Map<String, Object>> dateMap;

	public AbstractSerializerCacheGetter(DataFlowCacheConfig cacheConfig, ICacheStore cacheStore, ICacheStats cacheStats,
										 Map<String, Set<String>> indexMap, Map<String, Map<String, Object>> dateMap,
										 ClientMongoOperator clientMongoOperator) {
		super(cacheConfig, cacheStore, cacheStats, clientMongoOperator);
		this.indexMap = indexMap;
		this.dateMap = dateMap;
	}

	@Override
	protected List<Map<String, Object>> getRecordList(String cacheName, Object... cacheKeys) throws InterruptedException {
		Set<String> pkSet = indexMap.get(CacheUtil.cacheKey(cacheKeys));
		List<Map<String, Object>> resultList = new ArrayList<>();

		if (CollectionUtils.isNotEmpty(pkSet)) {
			for (String pk : pkSet) {
				Map<String, Object> record = dateMap.get(pk);
				resultList.add(record);
			}
		}
		return resultList;
	}

	/**
	 * 从缓存中取出一条记录
	 *
	 * @param cacheName
	 * @param cacheKeys
	 * @return
	 * @throws InterruptedException
	 */
	@Override
	protected Map<String, Object> getRecord(String cacheName, Object... cacheKeys) throws InterruptedException {
		// 等待所有cache节点初始化完成
		Set<String> pkSet = indexMap.get(CacheUtil.cacheKey(cacheKeys));
		Map<String, Object> result = null;

		if (CollectionUtils.isNotEmpty(pkSet)) {
			final Map<String, Object> dataRow = dateMap.get(pkSet.iterator().next());
			result = CacheUtil.returnCacheRow(dataRow);
		}
		return result;
	}

}
