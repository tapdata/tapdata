package com.tapdata.cache;

import com.tapdata.cache.exception.EncoderCacheNameException;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.dataflow.DataFlow;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.error.ShareCacheExCode_20;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.data.mongodb.core.query.Query;
import org.voovan.tools.collection.CacheMap;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public abstract class AbstractCacheService implements ICacheService {
	protected final Logger logger = LogManager.getLogger(AbstractCacheService.class);

	protected CacheMap<String, DataFlowCacheConfig> cacheConfigMap;

	/**
	 * getter 满 {@link #CACHE_GETTER_EXPIRE_MS} 后在下一次访问时重建以加载最新配置，替换下来的旧 getter 释放回源连接器。
	 * 不用 voovan CacheMap：它的过期重建不是原子的，定时器移除条目时也不触发淘汰回调，被覆盖或移除的 getter
	 * 不会再被释放，其回源连接器（MongoDB 连接器持有 MongoClient）永久泄漏（TAP-13150）
	 */
	private final Map<String, CacheGetterEntry> cacheGetterMap = new ConcurrentHashMap<>();

	// 每个 cacheName 一把 getter 替换锁，与 cacheDestroyLockMap 分开，避免读缓存等待耗时的物理销毁
	private final Map<String, Object> cacheGetterLockMap = new ConcurrentHashMap<>();

	static final long CACHE_GETTER_EXPIRE_MS = TimeUnit.SECONDS.toMillis(300);

	/**
	 * 释放旧 getter 会执行连接器 STOP，可能阻塞；放到独立的单线程执行，不占用读缓存的线程。
	 * 全进程共享：脚本试运行每次都会新建 MemoryCacheService
	 */
	private static final ExecutorService CACHE_GETTER_RELEASE_EXECUTOR = Executors.newSingleThreadExecutor(r -> {
		Thread thread = new Thread(r, "shared-cache-getter-release");
		thread.setDaemon(true);
		return thread;
	});

	private final Map<String, ICacheStore> cacheStoreMap;

	private final Map<String, Long> lastLogTSMap;

	private final Map<String, Lock> cacheStatusLockMap;

	/**
	 * 保存cache任务状态
	 */
	protected Map<String, String> cacheStatusMap;

	protected final ClientMongoOperator clientMongoOperator;

	public AbstractCacheService(ClientMongoOperator clientMongoOperator, Map<String, String> cacheStatusMap) {
		this(clientMongoOperator);
		this.cacheStatusMap = cacheStatusMap;
	}

	public AbstractCacheService(ClientMongoOperator clientMongoOperator) {
		this.clientMongoOperator = clientMongoOperator;
		this.cacheStoreMap = new ConcurrentHashMap<>();
		this.lastLogTSMap = new ConcurrentHashMap<>();
		this.cacheStatusLockMap = new ConcurrentHashMap<>();
		this.cacheConfigMap = new CacheMap<>();
		// 配置过期后用 supplier 向 TM 重新查询并原地刷新，不移除（autoRemove=false），getConfig 不会返回 null
		this.cacheConfigMap.maxSize(100).interval(60).expire(600).supplier(cacheName -> {
			String encodeCacheName;
			try {
				encodeCacheName = URLEncoder.encode(cacheName, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				throw new EncoderCacheNameException(ShareCacheExCode_20.ENCODE_CACHE_NAME, e).cacheName(cacheName).dynamicDescriptionParameters(cacheName);
			}
			// 如果不存在，则向tm查询
			TaskDto taskDto = clientMongoOperator.findOne(new Query(), ConnectorConstant.TASK_COLLECTION + "/byCacheName/" + encodeCacheName, TaskDto.class);
			DataFlowCacheConfig cacheConfig = CacheUtil.getCacheConfig(taskDto, clientMongoOperator);
			logger.warn("The cache task [{}] is abnormal, query by tm...", cacheName);
			if (logger.isDebugEnabled()) {
				logger.debug("query cache config is [{}]", cacheConfig);
			}
			if (cacheConfig != null) {
				this.cacheStatusMap.remove(cacheName);
				registerCache(cacheConfig);
			}
			return cacheConfig;
		}, false).create();

	}

	private ICacheGetter getCacheGetter(String cacheName) throws InterruptedException {
		waitCacheNodeCompletedInitialIfNeed(cacheName);
		CacheGetterEntry entry = this.cacheGetterMap.get(cacheName);
		if (entry != null && !entry.isExpired(currentTimeMillis())) {
			return entry.getter;
		}
		// 创建与替换按 cacheName 串行，保证同一时刻只有一个 getter，替换下来的旧 getter 都会被释放
		synchronized (this.cacheGetterLockMap.computeIfAbsent(cacheName, k -> new Object())) {
			entry = this.cacheGetterMap.get(cacheName);
			long now = currentTimeMillis();
			if (entry != null && !entry.isExpired(now)) {
				return entry.getter;
			}
			ICacheGetter getter = getCacheGetterInstance(cacheName);
			if (getter == null) {
				return null;
			}
			this.cacheGetterMap.put(cacheName, new CacheGetterEntry(getter, now));
			if (entry != null) {
				releaseExpiredGetter(cacheName, entry.getter);
			}
			return getter;
		}
	}

	/**
	 * 只释放旧 getter 的回源连接器，不调用 close：close 会销毁新旧 getter 共用的缓存数据
	 */
	private void releaseExpiredGetter(String cacheName, ICacheGetter getter) {
		Runnable release = () -> {
			try {
				getter.releaseDataSource();
			} catch (Exception e) {
				logger.warn("Release data source of expired cache getter [{}] failed: {}", cacheName, e.getMessage(), e);
			}
		};
		try {
			getCacheGetterReleaseExecutor().execute(release);
		} catch (RejectedExecutionException e) {
			logger.warn("Submit release of expired cache getter [{}] failed: {}", cacheName, e.getMessage());
		}
	}

	protected Executor getCacheGetterReleaseExecutor() {
		return CACHE_GETTER_RELEASE_EXECUTOR;
	}

	protected long currentTimeMillis() {
		return System.currentTimeMillis();
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
		this.cacheConfigMap.put(cacheName, cacheConfig);
		this.lastLogTSMap.put(cacheConfig.getCacheName(), 0L);
		this.cacheStatusMap.put(cacheName, DataFlow.STATUS_RUNNING);
	}

	@Override
	public synchronized void destroy(String cacheName) {
		try {
			ICacheStore cacheStore = this.getCacheStore(cacheName);
			if (cacheStore != null) {
				cacheStore.destroy();
			}
			this.cacheStatusMap.remove(cacheName);
			this.cacheConfigMap.remove(cacheName);
			CacheGetterEntry entry;
			synchronized (this.cacheGetterLockMap.computeIfAbsent(cacheName, k -> new Object())) {
				entry = this.cacheGetterMap.remove(cacheName);
			}
			if (entry != null) {
				entry.getter.close();
			}
		} catch (Exception e) {
			logger.error("cache destroy error: " + cacheName, e);
		}
	}

	@Override
	public Map<String, Object> getAndSetCache(String cacheName, Boolean lookup, Object... cacheKeys) throws Throwable {
		return getCacheGetter(cacheName).getAndSetCache(cacheName, lookup, cacheKeys);
	}

	@Override
	public List<Map<String, Object>> getAndSetCacheArray(String cacheName, Boolean lookup, Object... cacheKeys) throws Throwable {
		return getCacheGetter(cacheName).getAndSetCacheArray(cacheName, lookup, cacheKeys);
	}

	@Override
	public Map<String, Object> getCache(String cacheName, Boolean lookup, Object... cacheKeys) throws Throwable {
		return getCacheGetter(cacheName).getCache(cacheName, lookup, cacheKeys);
	}

	@Override
	public Object getCacheItem(String cacheName, String field, Object defaultValue, Object... cacheKeys) throws Throwable {
		return getCacheGetter(cacheName).getCacheItem(cacheName, field, defaultValue, cacheKeys);
	}

	@Override
	public DataFlowCacheConfig getConfig(String cacheName) {
		return cacheConfigMap.get(cacheName);
	}


	protected Map<String, ICacheStore> getCacheStoreMap() {
		return cacheStoreMap;
	}

	@Override
	public void cacheRow(String cacheName, String key, List<Map<String, Object>> rows) throws Throwable {
		getCacheStore(cacheName).cacheRow(cacheName, key, rows);
	}

	@Override
	public void removeByKey(String cacheName, String cacheKey, String pkKey) throws Throwable {
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

		if (this.cacheConfigMap.get(cacheName) == null) {
			// 如果不存在，则向tm查询
			TaskDto taskDto = clientMongoOperator.findOne(new Query(), ConnectorConstant.TASK_COLLECTION + "/byCacheName/" + cacheName, TaskDto.class);
			DataFlowCacheConfig cacheConfig = CacheUtil.getCacheConfig(taskDto, clientMongoOperator);
			if (cacheConfig == null) {
				throw new RuntimeException("cache not exist: " + cacheName);
			}
			logger.warn("The cache task [{}] is abnormal, query by tm...", cacheName);
			registerCache(cacheConfig);
			this.cacheStatusMap.put(cacheName, taskDto.getStatus());
		}

		//todo IMDG is not supported, locking is not necessary
//		Lock lock = getCacheStatusLock(cacheName);
//		if (lock.tryLock(1, TimeUnit.SECONDS)) {
			// 判断任务状态
//			try {
//				String status = this.cacheStatusMap.get(cacheName);
//				if (!StringUtils.equals(status, DataFlow.STATUS_RUNNING)) {
//          throw new RuntimeException("cache not complete initial:" + cacheName);
					//任务状态非正常时，提示用户启动任务，本次查询走查库
//					logger.warn("The cache task [{}] is abnormal, please check", cacheName);
//				}
//			} finally {
//				lock.unlock();
//			}
//		}

	}

	private static class CacheGetterEntry {
		private final ICacheGetter getter;
		private final long createTime;

		private CacheGetterEntry(ICacheGetter getter, long createTime) {
			this.getter = getter;
			this.createTime = createTime;
		}

		private boolean isExpired(long now) {
			return now - createTime >= CACHE_GETTER_EXPIRE_MS;
		}
	}
}
