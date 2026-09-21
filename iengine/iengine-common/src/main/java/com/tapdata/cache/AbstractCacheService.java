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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

public abstract class AbstractCacheService implements ICacheService {
	protected final Logger logger = LogManager.getLogger(AbstractCacheService.class);

	protected CacheMap<String, DataFlowCacheConfig> cacheConfigMap;

	private final CacheMap<String, ICacheGetter> cacheGetterMap;

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
		this.cacheGetterMap = new CacheMap<>();
		this.cacheGetterMap.destory((k, getter) -> {
			getter.close();
			return -1L;
		}).autoRemove(true).maxSize(100).interval(60).expire(300).supplier(this::getCacheGetterInstance).create();
		this.cacheStoreMap = new ConcurrentHashMap<>();
		this.lastLogTSMap = new ConcurrentHashMap<>();
		this.cacheStatusLockMap = new ConcurrentHashMap<>();
		this.cacheConfigMap = new CacheMap<>();
		this.cacheConfigMap.autoRemove(true).maxSize(100).interval(60).expire(600).supplier(cacheName -> {
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
		}).create();

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
		this.cacheConfigMap.put(cacheName, cacheConfig);
		this.lastLogTSMap.put(cacheConfig.getCacheName(), 0L);
		this.cacheStatusMap.put(cacheName, DataFlow.STATUS_RUNNING);
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
				this.cacheConfigMap.remove(cacheName);
				ICacheGetter cacheGetter = this.cacheGetterMap.remove(cacheName);
				if (cacheGetter != null) {
					cacheGetter.close();
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
}
