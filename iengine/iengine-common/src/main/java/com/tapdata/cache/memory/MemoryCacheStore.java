package com.tapdata.cache.memory;

import com.tapdata.cache.CacheUtil;
import com.tapdata.cache.ICacheStats;
import com.tapdata.cache.ICacheStore;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryCacheStore implements ICacheStore {

	private final Logger logger = LogManager.getLogger(MemoryCacheService.class);
	/**
	 * key: cache name
	 * value:
	 * - key: cache key
	 * - value:
	 * -- key: primary keys
	 * -- value: record
	 */
	private final Map<String, Map<String, Map<String, Object>>> cacheDataRows;

	private final ICacheStats cacheStats;

	private final DataFlowCacheConfig dataFlowCacheConfig;

	public MemoryCacheStore(Map<String, Map<String, Map<String, Object>>> cacheDataRows, ICacheStats cacheStats, DataFlowCacheConfig dataFlowCacheConfig) {
		this.cacheDataRows = cacheDataRows;
		this.cacheStats = cacheStats;
		this.dataFlowCacheConfig = dataFlowCacheConfig;
	}

	@Override
	public void cacheRow(String cacheName, String key, List<Map<String, Object>> rows) {
		if (cacheDataRows == null) {
			logger.warn("Does not have any cache by cache name {}, will abort this cache row {}.", cacheName, rows);
			return;
		}

		if (CollectionUtils.isEmpty(rows)) {
			return;
		}

		List<String> primaryKeys = dataFlowCacheConfig.getPrimaryKeys();

		Map<String, Map<String, Object>> pkRows = cacheDataRows.get(key);
		if (pkRows == null) {
			pkRows = new ConcurrentHashMap<>();
			cacheDataRows.put(key, pkRows);
		}
		for (Map<String, Object> row : rows) {
			Object[] pkValues = new Object[primaryKeys.size()];
			for (int i = 0; i < primaryKeys.size(); i++) {
				pkValues[i] = MapUtil.getValueByKey(row, primaryKeys.get(i));
			}
			Map<String, Object> previousAssociated = pkRows.put(
					CacheUtil.cacheKey(pkValues),
					row
			);

			if (previousAssociated == null) {
				cacheStats.addAndGetCacheRowCount(1);
			}

			if (cacheStats.getCacheRowCount() % 10000 == 0 &&
					!reachMaxCapacity(cacheName, row)) {

				long currentRowDataSize = RamUsageEstimator.sizeOfMap(row);
				// 记录不存在
				if (previousAssociated == null) {
					cacheStats.addAndGetCacheDataSize(currentRowDataSize);
				}
				// 记录已存在
				else {
					cacheStats.addAndGetCacheDataSize(currentRowDataSize);
					cacheStats.addAndGetCacheDataSize(
							RamUsageEstimator.sizeOfMap(previousAssociated) * -1
					);
				}

				// log info when data size reach 25000
//				logInfoCacheMetrics(cacheName, cacheStats, () -> cacheStats.getCacheRowCount().get() % LOG_INFO_BATCH_SIZE == 0);
			}
		}
	}

	@Override
	public void removeByKey(String cacheName, String cacheKey, String pkKey) {

		Map<String, Object> removedRow = null;
		if (StringUtils.isNotBlank(cacheKey)) {
			if (cacheDataRows.containsKey(cacheKey)) {
				Map<String, Map<String, Object>> pkRows = cacheDataRows.get(cacheKey);
				if (MapUtils.isNotEmpty(pkRows) && pkRows.containsKey(pkKey)) {
					removedRow = pkRows.remove(pkKey);
				}
				if (MapUtils.isEmpty(pkRows)) {
					cacheDataRows.remove(cacheKey);
				}
			}
		} else if (StringUtils.isNotBlank(pkKey)) {
			final Iterator<String> iterator = cacheDataRows.keySet().iterator();
			while (iterator.hasNext()) {
				final String key = iterator.next();
				final Map<String, Map<String, Object>> pkRows = cacheDataRows.get(key);
				if (pkRows != null && pkRows.containsKey(pkKey)) {
					removedRow = pkRows.remove(pkKey);
				}

				if (MapUtils.isEmpty(pkRows)) {
					cacheDataRows.remove(key);
				}

				if (removedRow != null) {
					break;
				}
			}
		}

//    if (removedRow != null) {
//      CacheStats cacheStats = getCacheStats(cacheName);
//      long currentRowDataSize = RamUsageEstimator.sizeOfMap(removedRow);
//      cacheStats.getCacheDataSize().getAndAdd(currentRowDataSize * -1);
//      cacheStats.getCacheRowCount().decrementAndGet();
//    }
	}

	@Override
	public void destroy() {
		this.cacheDataRows.clear();
	}

	/**
	 * 判断是否达到缓存阀值
	 *
	 * @param cacheName
	 * @return
	 */
	public synchronized boolean reachMaxCapacity(String cacheName, Map<String, Object> result) {

		long currentRowDataSize = RamUsageEstimator.sizeOfMap(result);
		boolean reachMaxCapacity = false;
		long maxRows = dataFlowCacheConfig.getMaxRows();
		long maxSize = dataFlowCacheConfig.getMaxSize();
		Map<String, Map<String, Map<String, Object>>> cacheRows = cacheDataRows;

		int removedRowsCount = 0;
		long removedRowSizeCount = 0L;
		while (maxRows > 0 && cacheStats.getCacheRowCount() > maxRows) {
			final Iterator<String> cacheDataRowsIter = cacheRows.keySet().iterator();
			while (cacheDataRowsIter.hasNext()) {
				final String cacheKey = cacheDataRowsIter.next();
				final Map<String, Map<String, Object>> pkRows = cacheRows.get(cacheKey);

				if (MapUtils.isNotEmpty(pkRows)) {
					final int size = pkRows.size();

					cacheStats.addAndGetCacheRowCount(size * -1);
					removedRowsCount = removedRowsCount + size;
					long removedDataSize = RamUsageEstimator.sizeOfMap(pkRows);
					cacheStats.addAndGetCacheDataSize(removedDataSize * -1);
					removedRowSizeCount = removedRowSizeCount + removedDataSize;

					pkRows.clear();
				}

				cacheRows.remove(cacheKey);

				if (cacheStats.getCacheRowCount() <= maxRows) {
					break;
				}
			}

			if (!reachMaxCapacity) {
				reachMaxCapacity = true;
			}

			if (MapUtils.isEmpty(cacheRows)) {
				break;
			}
		}

		// 第一次计算容量时，需要做全量计算，出于性能考虑
		long cacheDataSize = cacheStats.getCacheDataSize();
		if (cacheDataSize <= 0) {
			synchronized (this) {
				if (cacheDataSize <= 0) {
					logger.info("Calculating cache {} total data size.", cacheName);
					cacheDataSize = RamUsageEstimator.sizeOfMap(cacheRows);
					cacheStats.setCacheDataSize(cacheDataSize);
					logger.info("Finished calculated cache {} total data size, {}MB.", cacheName, CacheUtil.byteToMB(cacheDataSize));
				}
			}
		}

		if (maxSize > 0 && MapUtils.isNotEmpty(cacheRows)) {
			long dataSize = cacheDataSize + currentRowDataSize;
			// 循环删除，直到小于用户定义的容量大小
			while (CacheUtil.byteToMB(dataSize) > maxSize) {
				final Iterator<String> cacheDataRowsIter = cacheRows.keySet().iterator();
				while (cacheDataRowsIter.hasNext()) {
					final String cacheKey = cacheDataRowsIter.next();
					final Map<String, Map<String, Object>> pkRows = cacheRows.get(cacheKey);

					if (MapUtils.isNotEmpty(pkRows)) {

						long removedDataSize = RamUsageEstimator.sizeOfMap(pkRows);
						removedRowSizeCount += removedDataSize;
						dataSize = cacheStats.addAndGetCacheDataSize(removedDataSize * -1);

						pkRows.clear();
					}

					cacheRows.remove(cacheKey);
					if (CacheUtil.byteToMB(dataSize) <= maxSize) {
						break;
					}
				}

				if (!reachMaxCapacity) {
					reachMaxCapacity = true;
				}

				if (MapUtils.isEmpty(cacheRows)) {
					break;
				}
			}
		}

		if (removedRowsCount > 0 || removedRowSizeCount > 0) {
			logger.info(
					"Cache {} data size {}MB, data rows {}, max rows {}, removed {}kb data and {} rows.",
					cacheName,
					CacheUtil.byteToMB(cacheDataSize),
					cacheStats.getCacheRowCount(),
					maxRows,
					CacheUtil.byteToKB(removedRowSizeCount),
					removedRowsCount
			);
		}

		return reachMaxCapacity;
	}
}
