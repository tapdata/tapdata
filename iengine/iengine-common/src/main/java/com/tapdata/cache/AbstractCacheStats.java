package com.tapdata.cache;

public abstract class AbstractCacheStats implements ICacheStats {

	private long lastLogTs;

	/**
	 * 当缓存命中率低于此数值时，打印日志
	 */
	private final double LOG_INFO_HIT_CACHE_RATE_CAPACITY = 1.0D;

	private final long LOG_INFO_INTERVAL_MILLIS = 30 * 1000;

	abstract protected void hitStats(String cacheName, boolean hitCache);

	@Override
	public void hitRateStats(String cacheName, boolean hitCache) {
		hitStats(cacheName, hitCache);
		double hitRate = getHitRate();
		if (CacheUtil.logInfoCacheMetrics(cacheName, getCacheDataSize(), getCacheRowCount(), getHitCacheCount(), getMissCacheCount(),
				() -> hitRate <= LOG_INFO_HIT_CACHE_RATE_CAPACITY &&
						System.currentTimeMillis() - getLastLogTs() > LOG_INFO_INTERVAL_MILLIS
		)) {
			lastLogTs = System.currentTimeMillis();
		}
	}

	@Override
	public long getLastLogTs() {
		return lastLogTs;
	}
}
