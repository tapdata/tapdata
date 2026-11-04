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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

public abstract class AbstractCacheService implements ICacheService {
	protected final Logger logger = LogManager.getLogger(AbstractCacheService.class);

	// No timer or supplier runs on Guava's cache: refresh is driven only by an expired read.
	private final Cache<String, CacheConfigEntry> cacheConfigMap = CacheBuilder.newBuilder().maximumSize(100).build();
	private final Map<String, ConfigLoad> cacheConfigLoads = new ConcurrentHashMap<>();
	private final Cache<String, DataFlowCacheConfig> cacheInitializations = CacheBuilder.newBuilder().build();
	static final long CACHE_CONFIG_EXPIRE_MS = TimeUnit.SECONDS.toMillis(600);

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

	// 每个 cacheName 一把销毁锁，见 destroy(String)；与 cacheStatusLockMap 一样按 cacheName 常驻
	protected final Map<String, Object> cacheDestroyLockMap = new ConcurrentHashMap<>();

	public AbstractCacheService(ClientMongoOperator clientMongoOperator, Map<String, String> cacheStatusMap) {
		this(clientMongoOperator);
		this.cacheStatusMap = cacheStatusMap;
	}

	public AbstractCacheService(ClientMongoOperator clientMongoOperator) {
		this.clientMongoOperator = clientMongoOperator;
		this.cacheStoreMap = new ConcurrentHashMap<>();
		this.lastLogTSMap = new ConcurrentHashMap<>();
		this.cacheStatusLockMap = new ConcurrentHashMap<>();
	}

	private TaskDto loadCacheTaskFromTm(String cacheName) {
		try {
			String encodedName = URLEncoder.encode(cacheName, "UTF-8");
			return clientMongoOperator.findOne(new Query(), ConnectorConstant.TASK_COLLECTION + "/byCacheName/" + encodedName, TaskDto.class);
		} catch (UnsupportedEncodingException e) {
			throw new EncoderCacheNameException(ShareCacheExCode_20.ENCODE_CACHE_NAME, e).cacheName(cacheName).dynamicDescriptionParameters(cacheName);
		}
	}

	protected DataFlowCacheConfig loadConfigFromTm(String cacheName) {
		TaskDto task = loadCacheTaskFromTm(cacheName);
		if (task == null) {
			return null; // Only an absent task is a definitive miss.
		}
		DataFlowCacheConfig config = CacheUtil.getCacheConfigOrThrow(task, clientMongoOperator);
		if (config == null) {
			throw new IllegalStateException("Incomplete cache task configuration: " + cacheName);
		}
		return config;
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
		this.cacheConfigMap.asMap().compute(cacheName, (key, previous) -> {
			return new CacheConfigEntry(cacheConfig, currentTimeMillis());
		});
		onCacheRegistered(cacheConfig);
	}

	/** Side effects run after publication, without holding the cache segment lock. */
	protected void onCacheRegistered(DataFlowCacheConfig cacheConfig) {
		this.lastLogTSMap.put(cacheConfig.getCacheName(), 0L);
		this.cacheStatusMap.put(cacheConfig.getCacheName(), DataFlow.STATUS_RUNNING);
	}

	/**
	 * 按 cacheName 加锁，不能用实例级 synchronized。
	 * <p>
	 * 本方法体内做的是物理销毁：{@link ICacheStore#destroy()} 是无超时的集群操作，本地没有 store 时
	 * {@link #getCacheStore(String)} 还会经 {@link #getConfig(String)} 打一次同步 TM 往返。实例级锁会让
	 * 不同 cache 的销毁互相排队，一个卡住的销毁就挡住本节点其它共享缓存任务的清理，那些任务的启动随之
	 * 被无限期推迟（TAP-12865）。按 cacheName 加锁后，同一个 cache 的并发销毁仍然互斥，不同 cache 互不影响；
	 * 方法体内触碰的 cacheStatusMap / cacheConfigMap / cacheGetterMap 本身都是并发容器。
	 */
	@Override
	public void destroy(String cacheName) {
		synchronized (this.cacheDestroyLockMap.computeIfAbsent(cacheName, k -> new Object())) {
			try {
				ICacheStore cacheStore = this.getCacheStore(cacheName);
				if (cacheStore != null) {
					cacheStore.destroy();
				}
				this.cacheStatusMap.remove(cacheName);
				this.cacheConfigLoads.remove(cacheName);
				this.cacheConfigMap.invalidate(cacheName);
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
		CacheConfigEntry cached = cacheConfigMap.getIfPresent(cacheName);
		if (cached != null && currentTimeMillis() - cached.lastAttempt < CACHE_CONFIG_EXPIRE_MS) {
			return cached.config;
		}
		ConfigLoad candidate = new ConfigLoad(cacheName);
		ConfigLoad existing = cacheConfigLoads.putIfAbsent(cacheName, candidate);
		if (existing != null && (existing.owner == Thread.currentThread()
				|| (cached != null && existing.owner != null))) {
			// A refresh is already in flight; an available config must remain usable.
			return cached == null ? null : cached.config;
		}
		ConfigLoad load = existing == null ? candidate : existing;
		try {
			// FutureTask.run is idempotent: a follower can start an abandoned NEW task.
			load.result.run();
			return load.result.get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (ExecutionException e) {
			logger.warn("Load cache config [{}] failed; retaining the current config", cacheName, e.getCause());
		} finally {
			if (load.result.isDone()) {
				cacheConfigLoads.remove(cacheName, load);
			}
		}
		CacheConfigEntry current = cacheConfigMap.getIfPresent(cacheName);
		return current == null ? null : current.config;
	}

	private DataFlowCacheConfig refreshConfig(String cacheName, ConfigLoad load) {
		CacheConfigEntry before = cacheConfigMap.getIfPresent(cacheName);
		if (before != null && currentTimeMillis() - before.lastAttempt < CACHE_CONFIG_EXPIRE_MS) {
			return before.config;
		}
		if (before != null) {
			// Back off for one TTL even when TM is unavailable; keep serving the old config.
			before.lastAttempt = currentTimeMillis();
		}
		// No map lock is held across this synchronous TM request.
		DataFlowCacheConfig loaded = loadConfigFromTm(cacheName);
		CacheConfigEntry result = cacheConfigMap.asMap().compute(cacheName, (key, current) -> {
			// An explicit registration or destroy that happened during the query wins.
			if ((current != null && current != before) || cacheConfigLoads.get(cacheName) != load) {
				return current;
			}
			if (loaded == null) {
				return null;
			}
			return new CacheConfigEntry(loaded, currentTimeMillis());
		});
		if (result != null && result.config == loaded) {
			try {
				onCacheRegistered(loaded);
			} catch (Exception e) {
				logger.warn("Cache config [{}] stored but registration side effects failed", cacheName, e);
			}
		}
		return result == null ? null : result.config;
	}

	private final class ConfigLoad {
		private final FutureTask<DataFlowCacheConfig> result;
		private volatile Thread owner;

		private ConfigLoad(String cacheName) {
			result = new FutureTask<>(() -> {
				owner = Thread.currentThread();
				try {
					return refreshConfig(cacheName, this);
				} finally {
					owner = null;
				}
			});
		}
	}

	private static final class CacheConfigEntry {
		private final DataFlowCacheConfig config;
		private volatile long lastAttempt;

		private CacheConfigEntry(DataFlowCacheConfig config, long loadedAt) {
			this.config = config;
			this.lastAttempt = loadedAt;
		}
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

		if (getConfig(cacheName) == null) {
			DataFlowCacheConfig recovered = null;
			try {
				recovered = cacheInitializations.get(cacheName, () -> {
					DataFlowCacheConfig current = getConfig(cacheName);
					if (current != null) {
						return current;
					}
					TaskDto task = loadCacheTaskFromTm(cacheName);
					DataFlowCacheConfig config = task == null ? null : CacheUtil.getCacheConfigOrThrow(task, clientMongoOperator);
					if (config == null) {
						throw new IllegalStateException("cache not exist: " + cacheName);
					}
					registerCache(config);
					cacheStatusMap.put(cacheName, task.getStatus());
					return config;
				});
			} catch (ExecutionException e) {
				throw new IllegalStateException("Load cache configuration failed: " + cacheName, e.getCause());
			} finally {
				if (recovered != null) {
					cacheInitializations.asMap().remove(cacheName, recovered);
				}
			}
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
