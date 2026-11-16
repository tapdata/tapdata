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
import com.google.common.util.concurrent.UncheckedExecutionException;

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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

public abstract class AbstractCacheService implements ICacheService {
	protected final Logger logger = LogManager.getLogger(AbstractCacheService.class);

	// No timer or supplier runs on Guava's cache: refresh is driven only by an expired read.
	private final Cache<String, CacheConfigEntry> cacheConfigMap = CacheBuilder.newBuilder().maximumSize(100).build();
	private final Map<String, ConfigLoad> cacheConfigLoads = new ConcurrentHashMap<>();
	private final Cache<String, DataFlowCacheConfig> cacheInitializations = CacheBuilder.newBuilder().build();
	static final long CACHE_CONFIG_EXPIRE_MS = TimeUnit.SECONDS.toMillis(600);
	static final long CACHE_CONFIG_MISS_RETRY_MS = TimeUnit.SECONDS.toMillis(30);
	static final long CACHE_DESTROY_FENCE_WAIT_MS = TimeUnit.SECONDS.toMillis(5);
	protected static final long CACHE_DESTROY_FENCE_LEASE_SECONDS = 3;
	protected static final long CACHE_DESTROY_FENCE_RENEW_SECONDS = 1;
	private static final ScheduledExecutorService CACHE_DESTROY_FENCE_RENEWAL_EXECUTOR = Executors.newScheduledThreadPool(2, r -> {
		Thread thread = new Thread(r, "shared-cache-destroy-fence-renewal");
		thread.setDaemon(true);
		return thread;
	});

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
	private final Map<String, DataFlowCacheConfig> cacheStoreConfigMap = new ConcurrentHashMap<>();
	private final Map<String, String> cacheStoreGenerationMap = new ConcurrentHashMap<>();

	private final Map<String, Long> lastLogTSMap;

	private final Map<String, Lock> cacheStatusLockMap;

	/**
	 * 保存cache任务状态
	 */
	protected Map<String, String> cacheStatusMap;

	protected final ClientMongoOperator clientMongoOperator;
	private final Map<String, ReentrantReadWriteLock> cacheOperationLocks = new ConcurrentHashMap<>();

	// 每个 cacheName 一把销毁锁，见 destroy(String)；与 cacheStatusLockMap 一样按 cacheName 常驻
	protected final Map<String, Object> cacheDestroyLockMap = new ConcurrentHashMap<>();

	private ReentrantReadWriteLock cacheOperationLock(String cacheName) {
		return cacheOperationLocks.computeIfAbsent(cacheName, key -> new ReentrantReadWriteLock());
	}

	protected AutoCloseable acquireSharedCacheLifecycleLock(String cacheName) {
		return () -> { };
	}

	protected void finishSharedCacheDestroy(String cacheName, String generation) {
	}

	protected void publishSharedCacheDestroyFence(String cacheName, String generation) {
		publishSharedCacheGeneration(cacheName, generation);
	}

	protected void renewSharedCacheDestroyFence(String cacheName, String generation) {
	}

	private AutoCloseable startSharedCacheDestroyFenceRenewal(String cacheName, String generation) {
		ScheduledFuture<?> renewal = CACHE_DESTROY_FENCE_RENEWAL_EXECUTOR.scheduleAtFixedRate(() -> {
			try {
				renewSharedCacheDestroyFence(cacheName, generation);
			} catch (Exception e) {
				logger.warn("Unable to renew distributed cache destroy fence for [{}]: {}", cacheName, e.getMessage());
			}
		}, CACHE_DESTROY_FENCE_RENEW_SECONDS, CACHE_DESTROY_FENCE_RENEW_SECONDS, TimeUnit.SECONDS);
		return () -> renewal.cancel(false);
	}

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
		if (usesDistributedCacheGeneration()) return getGenerationAwareCacheGetter(cacheName);
		return getLegacyCacheGetter(cacheName);
	}

	protected boolean usesDistributedCacheGeneration() {
		return false;
	}

	protected ICacheGetter getCacheGetterInstance(String cacheName, DataFlowCacheConfig config, ICacheStore store) {
		return getCacheGetterInstance(cacheName);
	}

	private ICacheGetter getGenerationAwareCacheGetter(String cacheName) throws InterruptedException {
		waitCacheNodeCompletedInitialIfNeed(cacheName);
		for (int attempt = 0; attempt < 5; attempt++) {
			DataFlowCacheConfig config = getConfig(cacheName);
			if (config == null) return null;
			String generation = sharedCacheGeneration(cacheName);
			CacheGetterEntry cached = cacheGetterMap.get(cacheName);
			if (cached != null && !cached.isExpired(currentTimeMillis()) && cached.config == config
					&& java.util.Objects.equals(cached.generation, generation)
					&& cacheStoreMap.containsKey(cacheName) && cacheStoreConfigMap.get(cacheName) == config) {
				return cached.getter;
			}
			ICacheStore store = getCacheStore(cacheName);
			generation = cacheStoreGenerationMap.get(cacheName);
			if (store == null) return null;
			Lock writeLock = cacheOperationLock(cacheName).writeLock();
			writeLock.lock();
			try {
				synchronized (cacheLifecycleLock(cacheName)) {
				if (!java.util.Objects.equals(generation, sharedCacheGeneration(cacheName))
						|| cacheStoreMap.get(cacheName) != store || cacheConfigMap.getIfPresent(cacheName) == null
						|| cacheConfigMap.getIfPresent(cacheName).config != config
						|| cacheStoreConfigMap.get(cacheName) != config) continue;
				synchronized (cacheGetterLockMap.computeIfAbsent(cacheName, key -> new Object())) {
					CacheGetterEntry entry = cacheGetterMap.get(cacheName);
					long now = currentTimeMillis();
					if (entry != null && !entry.isExpired(now) && entry.config == config
							&& java.util.Objects.equals(entry.generation, generation)) return entry.getter;
					ICacheGetter getter = getCacheGetterInstance(cacheName, config, store);
					if (getter == null) return null;
					cacheGetterMap.put(cacheName, new CacheGetterEntry(getter, now, config, generation));
					if (entry != null) releaseExpiredGetter(cacheName, entry.getter);
					return getter;
				}
			}
			} finally {
				writeLock.unlock();
			}
		}
		throw new IllegalStateException("Cache generation changed repeatedly while creating its getter: " + cacheName);
	}

	private ICacheGetter getLegacyCacheGetter(String cacheName) throws InterruptedException {
		waitCacheNodeCompletedInitialIfNeed(cacheName);
		CacheGetterEntry entry = this.cacheGetterMap.get(cacheName);
		if (entry != null && !entry.isExpired(currentTimeMillis())) {
			return entry.getter;
		}
		// 创建与替换按 cacheName 串行，保证同一时刻只有一个 getter，替换下来的旧 getter 都会被释放
		Lock writeLock = cacheOperationLock(cacheName).writeLock();
		writeLock.lock();
		try {
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
		} finally {
			writeLock.unlock();
		}
	}

	@FunctionalInterface
	private interface GetterOperation<T> {
		T apply(ICacheGetter getter) throws Throwable;
	}

	@FunctionalInterface
	private interface StoreOperation<T> {
		T apply(ICacheStore store) throws Throwable;
	}

	private <T> T withCacheGetter(String cacheName, GetterOperation<T> operation) throws Throwable {
		for (int attempt = 0; attempt < 5; attempt++) {
			ICacheGetter getter = getCacheGetter(cacheName);
			if (getter == null) throw new IllegalStateException("Cache getter is unavailable: " + cacheName);
			Lock readLock = cacheOperationLock(cacheName).readLock();
			readLock.lock();
			try {
				CacheGetterEntry entry = cacheGetterMap.get(cacheName);
				if (entry == null || entry.getter != getter) continue;
				if (usesDistributedCacheGeneration()
						&& !java.util.Objects.equals(entry.generation, sharedCacheGeneration(cacheName))) continue;
				return operation.apply(getter);
			} finally {
				readLock.unlock();
			}
		}
		throw new IllegalStateException("Cache generation changed repeatedly while reading: " + cacheName);
	}

	private <T> T withCacheStore(String cacheName, StoreOperation<T> operation) throws Throwable {
		if (!usesDistributedCacheGeneration()) return operation.apply(getCacheStore(cacheName));
		for (int attempt = 0; attempt < 5; attempt++) {
			ICacheStore store = getCacheStore(cacheName);
			if (store == null) throw new IllegalStateException("Cache store is unavailable: " + cacheName);
			Lock readLock = cacheOperationLock(cacheName).readLock();
			readLock.lock();
			try {
				if (cacheStoreMap.get(cacheName) != store
						|| !java.util.Objects.equals(cacheStoreGenerationMap.get(cacheName), sharedCacheGeneration(cacheName))) continue;
				return operation.apply(store);
			} finally {
				readLock.unlock();
			}
		}
		throw new IllegalStateException("Cache generation changed repeatedly while writing: " + cacheName);
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
		Lock writeLock = cacheOperationLock(cacheName).writeLock();
		writeLock.lock();
		try {
			long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(CACHE_DESTROY_FENCE_WAIT_MS);
			while (true) {
				boolean destroying;
				try (AutoCloseable ignored = acquireSharedCacheLifecycleLock(cacheName)) {
					synchronized (cacheLifecycleLock(cacheName)) {
						String currentGeneration = sharedCacheGeneration(cacheName);
						destroying = currentGeneration != null && currentGeneration.startsWith("destroying:");
						if (!destroying) {
							cacheConfigMap.put(cacheName, new CacheConfigEntry(cacheConfig, currentTimeMillis()));
							onCacheRegistered(cacheConfig);
							String generation = "active:" + UUID.randomUUID();
							publishSharedCacheGeneration(cacheName, generation);
							cacheStoreGenerationMap.put(cacheName, generation);
							return;
						}
					}
				} catch (Exception e) {
					throw new IllegalStateException("Unable to register cache: " + cacheName, e);
				}
				long remainingNanos = deadline - System.nanoTime();
				if (!destroying || remainingNanos <= 0) {
					throw new IllegalStateException("Cache is still being destroyed after bounded wait: " + cacheName);
				}
				try {
					TimeUnit.NANOSECONDS.sleep(Math.min(remainingNanos, TimeUnit.MILLISECONDS.toNanos(100)));
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new IllegalStateException("Interrupted waiting for cache destroy: " + cacheName, e);
				}
			}
		} finally {
			writeLock.unlock();
		}
	}

	private Object cacheLifecycleLock(String cacheName) {
		return cacheDestroyLockMap.computeIfAbsent(cacheName, key -> new Object());
	}

	protected final Object getCacheLifecycleLock(String cacheName) {
		return cacheLifecycleLock(cacheName);
	}

	/**
	 * Hazelcast-backed services override these hooks to publish a per-cache generation to every engine node.
	 * Local cache implementations retain the default process-local behavior.
	 */
	protected String sharedCacheGeneration(String cacheName) {
		return null;
	}

	protected void publishSharedCacheGeneration(String cacheName, String generation) {
	}

	protected final ICacheStore getOrCreateCacheStore(String cacheName, Function<DataFlowCacheConfig, ICacheStore> factory,
													 boolean failWhenConfigMissing) {
		for (int attempt = 0; attempt < 5; attempt++) {
			String generation = sharedCacheGeneration(cacheName);
			if (generation != null && generation.startsWith("destroying:")) {
				invalidateLocalCacheGeneration(cacheName, generation);
				if (failWhenConfigMissing) throw new IllegalStateException("Cache is being destroyed: " + cacheName);
				return null;
			}
			String knownGeneration = cacheStoreGenerationMap.get(cacheName);
			if (!java.util.Objects.equals(knownGeneration, generation)
					&& (cacheStoreMap.containsKey(cacheName) || cacheStoreConfigMap.containsKey(cacheName)
					|| cacheConfigMap.getIfPresent(cacheName) != null || cacheGetterMap.containsKey(cacheName))) {
				invalidateLocalCacheGeneration(cacheName, generation);
			} else if (generation != null) {
				cacheStoreGenerationMap.put(cacheName, generation);
			}

			// Config refresh may block on TM. Never perform it while holding the per-cache lifecycle lock.
			DataFlowCacheConfig config = getConfig(cacheName);
			if (config == null) {
				invalidateLocalCacheGeneration(cacheName, generation);
				if (failWhenConfigMissing) throw new IllegalStateException("Cache configuration is unavailable: " + cacheName);
				return null;
			}
			Lock writeLock = cacheOperationLock(cacheName).writeLock();
			writeLock.lock();
			try {
				synchronized (cacheLifecycleLock(cacheName)) {
				String currentGeneration = sharedCacheGeneration(cacheName);
				if (!java.util.Objects.equals(generation, currentGeneration)) continue;
				CacheConfigEntry configEntry = cacheConfigMap.getIfPresent(cacheName);
				if (configEntry == null || configEntry.config != config) continue;
				ICacheStore current = cacheStoreMap.get(cacheName);
				if (current != null && cacheStoreConfigMap.get(cacheName) == config
						&& java.util.Objects.equals(cacheStoreGenerationMap.get(cacheName), generation)) {
					return current;
				}
				if (current != null) {
					cacheStoreMap.remove(cacheName, current);
					cacheStoreConfigMap.remove(cacheName);
					cacheStoreGenerationMap.remove(cacheName);
					invalidateCacheGetter(cacheName);
				}
				ICacheStore created = factory.apply(config);
				if (created != null) {
					cacheStoreMap.put(cacheName, created);
					cacheStoreConfigMap.put(cacheName, config);
					if (generation != null) cacheStoreGenerationMap.put(cacheName, generation);
				}
				return created;
			}
			} finally {
				writeLock.unlock();
			}
		}
		throw new IllegalStateException("Cache configuration changed repeatedly while creating its store: " + cacheName);
	}

	private void invalidateLocalCacheGeneration(String cacheName, String generation) {
		ICacheGetter oldGetter = null;
		Lock writeLock = cacheOperationLock(cacheName).writeLock();
		writeLock.lock();
		try {
			synchronized (cacheLifecycleLock(cacheName)) {
			String current = sharedCacheGeneration(cacheName);
			if (!java.util.Objects.equals(generation, current)) return;
			cacheStoreMap.remove(cacheName);
			cacheStoreConfigMap.remove(cacheName);
			if (generation != null) cacheStoreGenerationMap.put(cacheName, generation);
			cacheConfigLoads.remove(cacheName);
			cacheConfigMap.invalidate(cacheName);
			synchronized (cacheGetterLockMap.computeIfAbsent(cacheName, key -> new Object())) {
				CacheGetterEntry entry = cacheGetterMap.remove(cacheName);
				if (entry != null) oldGetter = entry.getter;
			}
		}
		} finally {
			writeLock.unlock();
		}
		if (oldGetter != null) releaseExpiredGetter(cacheName, oldGetter);
	}

	private void invalidateCacheGetter(String cacheName) {
		CacheGetterEntry old;
		synchronized (cacheGetterLockMap.computeIfAbsent(cacheName, key -> new Object())) {
			old = cacheGetterMap.remove(cacheName);
		}
		if (old != null) releaseExpiredGetter(cacheName, old.getter);
	}

	private boolean configIsFresh(CacheConfigEntry entry) {
		long ttl = entry.config == null ? CACHE_CONFIG_MISS_RETRY_MS : CACHE_CONFIG_EXPIRE_MS;
		return currentTimeMillis() - entry.lastAttempt < ttl;
	}

	private boolean hasRecentConfigMiss(String cacheName) {
		CacheConfigEntry entry = cacheConfigMap.getIfPresent(cacheName);
		return entry != null && entry.config == null && configIsFresh(entry);
	}

	private void rememberConfigMiss(String cacheName) {
		synchronized (cacheLifecycleLock(cacheName)) {
			CacheConfigEntry current = cacheConfigMap.getIfPresent(cacheName);
			if (current == null || current.config == null) {
				cacheConfigMap.put(cacheName, new CacheConfigEntry(null, currentTimeMillis()));
			}
		}
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
		ICacheStore preloaded = cacheStoreMap.get(cacheName);
		if (preloaded == null) {
			try {
				preloaded = getCacheStore(cacheName);
			} catch (Exception e) {
				logger.warn("Unable to preload cache store [{}] before destroy; local cleanup will still run: {}", cacheName, e.getMessage());
			}
		}
		Lock writeLock = cacheOperationLock(cacheName).writeLock();
		writeLock.lock(); // Wait for local reads before acquiring the distributed lock.
		String destroyingGeneration = null;
		ICacheStore store;
		CacheGetterEntry entry;
		try {
			synchronized (cacheLifecycleLock(cacheName)) {
				try (AutoCloseable ignored = acquireSharedCacheLifecycleLock(cacheName)) {
					String currentGeneration = sharedCacheGeneration(cacheName);
					if (currentGeneration == null || !currentGeneration.startsWith("destroying:v2:")) {
						String candidateGeneration = "destroying:v2:" + currentTimeMillis() + ":" + UUID.randomUUID();
						publishSharedCacheDestroyFence(cacheName, candidateGeneration);
						destroyingGeneration = candidateGeneration;
					}
				} catch (Exception e) {
					logger.warn("Unable to acquire distributed cache lifecycle lock for [{}]; local cleanup will continue without cluster destroy: {}",
							cacheName, e.getMessage());
				}
				store = cacheStoreMap.remove(cacheName);
				if (store == null && destroyingGeneration != null && preloaded != null
						&& cacheStoreConfigMap.get(cacheName) != null) store = preloaded;
				cacheStoreConfigMap.remove(cacheName);
				cacheStatusMap.remove(cacheName);
				cacheConfigLoads.remove(cacheName);
				cacheConfigMap.invalidate(cacheName);
				synchronized (cacheGetterLockMap.computeIfAbsent(cacheName, key -> new Object())) {
					entry = cacheGetterMap.remove(cacheName);
				}
				if (destroyingGeneration != null) cacheStoreGenerationMap.put(cacheName, destroyingGeneration);
				else cacheStoreGenerationMap.remove(cacheName);
			}

			if (destroyingGeneration != null) {
				AutoCloseable fenceRenewal = startSharedCacheDestroyFenceRenewal(cacheName, destroyingGeneration);
				try {
					if (store != null) store.destroy();
				} catch (Exception e) {
					logger.error("cache destroy error: " + cacheName, e);
				} finally {
					try {
						fenceRenewal.close();
					} catch (Exception e) {
						logger.warn("Unable to stop cache destroy fence renewal for [{}]: {}", cacheName, e.getMessage());
					}
					try (AutoCloseable ignored = acquireSharedCacheLifecycleLock(cacheName)) {
						finishSharedCacheDestroy(cacheName, destroyingGeneration);
						cacheStoreGenerationMap.remove(cacheName, destroyingGeneration);
					} catch (Exception e) {
						logger.warn("Unable to finalize distributed cache destroy for [{}]; the lease will expire and a later destroy can recover: {}",
								cacheName, e.getMessage());
					}
				}
			}
		} finally {
			writeLock.unlock();
		}

		// Connector shutdown is local and may block; do not keep a cluster-wide lock while closing it.
		if (entry != null) {
			try {
				entry.getter.close();
			} catch (Exception e) {
				logger.error("cache getter close error: " + cacheName, e);
			}
		}
	}

	@Override
	public Map<String, Object> getAndSetCache(String cacheName, Boolean lookup, Object... cacheKeys) throws Throwable {
		return withCacheGetter(cacheName, getter -> getter.getAndSetCache(cacheName, lookup, cacheKeys));
	}

	@Override
	public List<Map<String, Object>> getAndSetCacheArray(String cacheName, Boolean lookup, Object... cacheKeys) throws Throwable {
		return withCacheGetter(cacheName, getter -> getter.getAndSetCacheArray(cacheName, lookup, cacheKeys));
	}

	@Override
	public Map<String, Object> getCache(String cacheName, Boolean lookup, Object... cacheKeys) throws Throwable {
		return withCacheGetter(cacheName, getter -> getter.getCache(cacheName, lookup, cacheKeys));
	}

	@Override
	public Object getCacheItem(String cacheName, String field, Object defaultValue, Object... cacheKeys) throws Throwable {
		return withCacheGetter(cacheName, getter -> getter.getCacheItem(cacheName, field, defaultValue, cacheKeys));
	}

	@Override
	public DataFlowCacheConfig getConfig(String cacheName) {
		CacheConfigEntry cached = cacheConfigMap.getIfPresent(cacheName);
		if (cached != null && configIsFresh(cached)) {
			return cached.config;
		}
		ConfigLoad candidate = new ConfigLoad(cacheName);
		ConfigLoad existing = cacheConfigLoads.putIfAbsent(cacheName, candidate);
		if (existing != null && cached != null) {
			// Stale readers never wait for an active refresh, including its startup window.
			// A terminated creator cannot start a NEW task, so this caller may take over.
			if (!existing.creator.isAlive()) {
				existing.result.run();
			}
			if (existing.result.isDone()) {
				cacheConfigLoads.remove(cacheName, existing);
			}
			CacheConfigEntry current = cacheConfigMap.getIfPresent(cacheName);
			return current == null ? null : current.config;
		}
		if (existing != null && existing.owner == Thread.currentThread()) {
			return null;
		}
		ConfigLoad load = existing == null ? candidate : existing;
		try {
			// FutureTask.run is idempotent: a follower can start an abandoned NEW task.
			load.result.run();
			return load.result.get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (ExecutionException e) {
			synchronized (cacheLifecycleLock(cacheName)) {
				if (cacheConfigLoads.get(cacheName) == load && cacheConfigMap.getIfPresent(cacheName) == null) {
					cacheConfigMap.put(cacheName, new CacheConfigEntry(null, currentTimeMillis()));
				}
			}
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
		if (before != null && configIsFresh(before)) {
			return before.config;
		}
		if (before != null) {
			// Back off for one TTL even when TM is unavailable; keep serving the old config.
			before.lastAttempt = currentTimeMillis();
		}
		// No map lock is held across this synchronous TM request.
		DataFlowCacheConfig loaded = loadConfigFromTm(cacheName);
		synchronized (cacheLifecycleLock(cacheName)) {
			CacheConfigEntry current = cacheConfigMap.getIfPresent(cacheName);
			if ((current != null && current != before) || cacheConfigLoads.get(cacheName) != load) {
				return current == null ? null : current.config;
			}
			// Publishing and side effects share destroy's per-cache lock, never Guava's segment lock.
			cacheConfigMap.put(cacheName, new CacheConfigEntry(loaded, currentTimeMillis()));
			if (loaded != null) {
				try {
					onCacheRegistered(loaded);
				} catch (Exception e) {
					logger.warn("Cache config [{}] stored but registration side effects failed", cacheName, e);
				}
			}
			return loaded;
		}
	}

	private final class ConfigLoad {
		private final FutureTask<DataFlowCacheConfig> result;
		private volatile Thread owner;
		private final Thread creator = Thread.currentThread();

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
		withCacheStore(cacheName, store -> { store.cacheRow(cacheName, key, rows); return null; });
	}

	@Override
	public void removeByKey(String cacheName, String cacheKey, String pkKey) throws Throwable {
		withCacheStore(cacheName, store -> { store.removeByKey(cacheName, cacheKey, pkKey); return null; });
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
			if (hasRecentConfigMiss(cacheName)) {
				throw new RuntimeException("cache not exist: " + cacheName);
			}
			DataFlowCacheConfig recovered = null;
			try {
				recovered = cacheInitializations.get(cacheName, () -> {
					DataFlowCacheConfig current = getConfig(cacheName);
					if (current != null) {
						return current;
					}
					if (hasRecentConfigMiss(cacheName)) {
						throw new RuntimeException("cache not exist: " + cacheName);
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
			} catch (ExecutionException | UncheckedExecutionException e) {
				rememberConfigMiss(cacheName);
				throw new RuntimeException("cache not exist: " + cacheName, e.getCause());
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
		private final DataFlowCacheConfig config;
		private final String generation;

		private CacheGetterEntry(ICacheGetter getter, long createTime) {
			this(getter, createTime, null, null);
		}

		private CacheGetterEntry(ICacheGetter getter, long createTime, DataFlowCacheConfig config, String generation) {
			this.getter = getter;
			this.createTime = createTime;
			this.config = config;
			this.generation = generation;
		}

		private boolean isExpired(long now) {
			return now - createTime >= CACHE_GETTER_EXPIRE_MS;
		}
	}
}
