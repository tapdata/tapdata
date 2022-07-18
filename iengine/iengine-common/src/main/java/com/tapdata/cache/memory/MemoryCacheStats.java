package com.tapdata.cache.memory;

import com.tapdata.cache.AbstractCacheStats;

import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryCacheStats extends AbstractCacheStats {
	private AtomicLong cacheRowCount;
	private AtomicLong cacheDataSize;
	private AtomicLong hitCacheCount;
	private AtomicLong missCacheCount;

	public MemoryCacheStats(
			AtomicLong cacheRowCount,
			AtomicLong cacheDataSize,
			AtomicLong hitCacheCount,
			AtomicLong missCacheCount

	) {
		this.cacheRowCount = cacheRowCount;
		this.cacheDataSize = cacheDataSize;
		this.hitCacheCount = hitCacheCount;
		this.missCacheCount = missCacheCount;
	}

	@Override
	public void hitStats(String cacheName, boolean hitCache) {
		if (hitCache) {
			this.hitCacheCount.incrementAndGet();
		} else {
			this.missCacheCount.incrementAndGet();
		}
	}

	@Override
	public double getHitRate() {
		long hitCacheCount = this.hitCacheCount.get();
		long missCacheCount = this.missCacheCount.get();
		if (hitCacheCount == 0 && missCacheCount == 0) {
			return 0;
		}
		double hitRate = (double) hitCacheCount / (hitCacheCount + missCacheCount);
		return new BigDecimal(hitRate).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
	}

	@Override
	public long addAndGetCacheRowCount(long rowCount) {
		return this.cacheRowCount.addAndGet(rowCount);
	}

	@Override
	public long addAndGetCacheDataSize(long dataSize) {
		return this.cacheDataSize.addAndGet(dataSize);
	}

	public long getCacheRowCount() {
		return cacheRowCount.longValue();
	}

	public void setCacheRowCount(AtomicLong cacheRowCount) {
		this.cacheRowCount = cacheRowCount;
	}

	public long getCacheDataSize() {
		return cacheDataSize.longValue();
	}

	@Override
	public void setCacheDataSize(long dataSize) {
		this.cacheDataSize.set(dataSize);
	}

	public void setCacheDataSize(AtomicLong cacheDataSize) {
		this.cacheDataSize = cacheDataSize;
	}

	public long getHitCacheCount() {
		return hitCacheCount.longValue();
	}

	public void setHitCacheCount(AtomicLong hitCacheCount) {
		this.hitCacheCount = hitCacheCount;
	}

	public long getMissCacheCount() {
		return missCacheCount.longValue();
	}


	public void setMissCacheCount(AtomicLong missCacheCount) {
		this.missCacheCount = missCacheCount;
	}
}
