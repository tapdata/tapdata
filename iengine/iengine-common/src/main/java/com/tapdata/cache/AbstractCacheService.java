package com.tapdata.cache;

import com.tapdata.entity.dataflow.DataFlow;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public abstract class AbstractCacheService implements ICacheService {
	protected final Logger logger = LogManager.getLogger(AbstractCacheService.class);

	protected Map<String, DataFlowCacheConfig> cacheConfigMap;

	private Map<String, ICacheStats> cacheStatsMap;

	private final Map<String, ICacheGetter> cacheGetterMap;

	private final Map<String, ICacheStore> cacheStoreMap;

	private final Map<String, Long> lastLogTSMap;

	private final Map<String, Lock> cacheStatusLockMap;

	/**
	 * 保存cache任务状态
	 */
	protected Map<String, String> cacheStatusMap;

	public AbstractCacheService() {
		this.cacheGetterMap = new ConcurrentHashMap<>();
		this.cacheStoreMap = new ConcurrentHashMap<>();
		this.lastLogTSMap = new ConcurrentHashMap<>();
		this.cacheStatsMap = new ConcurrentHashMap<>();
		this.cacheStatusLockMap = new ConcurrentHashMap<>();
	}

	private ICacheGetter getCacheGetter(String cacheName) throws InterruptedException {
		waitCacheNodeCompletedInitialIfNeed(cacheName);
		return this.cacheGetterMap.computeIfAbsent(cacheName, f -> getCacheGetterInstance(cacheName));
	}

	private Lock getCacheStatusLock(String cacheName) {
		return this.cacheStatusLockMap.computeIfAbsent(cacheName, f -> getCacheStatusLockInstance(cacheName));
	}

	protected abstract Lock getCacheStatusLockInstance(String cacheName);

	abstract protected ICacheGetter getCacheGetterInstance(String cacheName);

	abstract protected ICacheStats getCacheStats(String cacheName);

	abstract protected ICacheStore getCacheStore(String cacheName);

	@Override
	public void registerCache(DataFlowCacheConfig cacheConfig) {
		String cacheName = cacheConfig.getCacheName();
		if (this.cacheConfigMap.containsKey(cacheName)) {
			throw new RuntimeException(String.format("Cache name %s already exists.", cacheName));
		}

		this.cacheConfigMap.put(cacheName, cacheConfig);
		this.lastLogTSMap.put(cacheConfig.getCacheName(), 0L);
		this.cacheStatusMap.put(cacheName, DataFlow.STATUS_RUNNING);
	}

	@Override
	public synchronized void destroy(String cacheName) {
		try {
			this.getCacheStore(cacheName).destroy();
			this.cacheStatusMap.remove(cacheName);
			this.cacheConfigMap.remove(cacheName);
		} catch (Exception e) {
			logger.error("cache destroy error: " + cacheName, e);
		}
	}

	@Override
	public Map<String, Object> getAndSetCache(String cacheName, Boolean lookup, Object... cacheKeys) throws InterruptedException {
		return getCacheGetter(cacheName).getAndSetCache(cacheName, lookup, cacheKeys);
	}

	@Override
	public List<Map<String, Object>> getAndSetCacheArray(String cacheName, Boolean lookup, Object... cacheKeys) throws InterruptedException {
		return getCacheGetter(cacheName).getAndSetCacheArray(cacheName, lookup, cacheKeys);
	}

	@Override
	public Map<String, Object> getCache(String cacheName, Boolean lookup, Object... cacheKeys) throws InterruptedException {
		return getCacheGetter(cacheName).getCache(cacheName, lookup, cacheKeys);
	}

	@Override
	public Object getCacheItem(String cacheName, String field, Object defaultValue, Object... cacheKeys) throws InterruptedException {
		return getCacheGetter(cacheName).getCacheItem(cacheName, field, defaultValue, cacheKeys);
	}

	@Override
	public DataFlowCacheConfig getConfig(String cacheName) {
		return cacheConfigMap.get(cacheName);
	}

	protected Map<String, ICacheStats> getCacheStatsMap() {
		return cacheStatsMap;
	}

	public void setCacheStatsMap(Map<String, ICacheStats> cacheStatsMap) {
		this.cacheStatsMap = cacheStatsMap;
	}


	protected Map<String, ICacheStore> getCacheStoreMap() {
		return cacheStoreMap;
	}

	@Override
	public void cacheRow(String cacheName, String key, List<Map<String, Object>> rows) {
		getCacheStore(cacheName).cacheRow(cacheName, key, rows);
	}

	@Override
	public void removeByKey(String cacheName, String cacheKey, String pkKey) {
		getCacheStore(cacheName).removeByKey(cacheName, cacheKey, pkKey);
	}

	@Override
	public void updateCacheStatus(String cacheName, String status) {
		Lock lock = getCacheStatusLock(cacheName);
		lock.lock();
		try {
			this.cacheStatusMap.put(cacheName, status);
		} finally {
			lock.unlock();
		}
	}

	private void waitCacheNodeCompletedInitialIfNeed(String cacheName) throws InterruptedException {
//    while (this.cacheConfigMap.get(cacheName) == null && !Thread.interrupted()) {
//      logger.warn("Waiting cache node complete registered: " + cacheName);
//      TimeUnit.SECONDS.sleep(3L);
//    }
		if (this.cacheConfigMap.get(cacheName) == null) {
			throw new RuntimeException("cache not registered: " + cacheName);
		}

		Lock lock = getCacheStatusLock(cacheName);
		if (lock.tryLock(1, TimeUnit.SECONDS)) {
			// 判断任务状态
			try {
				String status = this.cacheStatusMap.get(cacheName);
				if (!StringUtils.equals(status, DataFlow.STATUS_RUNNING)) {
					throw new RuntimeException("cache not complete initial:" + cacheName);
				}
			} finally {
				lock.unlock();
			}
		}
//    while (!Thread.interrupted()) {
//      if (lock.tryLock(1, TimeUnit.SECONDS)) {
//        // 判断任务状态
//        try {
//          String status = this.cacheStatusMap.get(cacheName);
//          if (StringUtils.equals(status, DataFlow.STATUS_RUNNING)) {
//            break;
//          }
//        } finally {
//          lock.unlock();
//        }
//      }
//      logger.warn("Waiting all cache node complete initial: " + cacheName);
//
//      TimeUnit.SECONDS.sleep(3);
//    }

	}
}
