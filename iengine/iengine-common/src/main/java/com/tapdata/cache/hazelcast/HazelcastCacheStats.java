package com.tapdata.cache.hazelcast;

import com.tapdata.cache.AbstractCacheStats;

public class HazelcastCacheStats extends AbstractCacheStats {


	private long cacheDataSize;

	private long cacheRowCount;

	private long hitCacheCount;

	private long missCacheCount;

	public HazelcastCacheStats() {

	}

	public HazelcastCacheStats(long cacheDataSize, long cacheRowCount, long hitCacheCount, long missCacheCount) {
		this.cacheDataSize = cacheDataSize;
		this.cacheRowCount = cacheRowCount;
		this.hitCacheCount = hitCacheCount;
		this.missCacheCount = missCacheCount;
	}

	@Override
	public long getCacheDataSize() {
		return cacheDataSize;
	}

	@Override
	public void setCacheDataSize(long dataSize) {
		this.cacheDataSize = dataSize;
	}

	@Override
	public long getCacheRowCount() {
		return cacheRowCount;
	}

	@Override
	public long getHitCacheCount() {
		return hitCacheCount;
	}

	@Override
	public long getMissCacheCount() {
		return missCacheCount;
	}

	@Override
	public void hitStats(String cacheName, boolean hitCache) {
		if (hitCache) {
			hitCacheCount++;
		} else {
			missCacheCount++;
		}
	}

	@Override
	public double getHitRate() {
		return hitCacheCount * 1.0 / cacheRowCount;
	}

	@Override
	public long addAndGetCacheRowCount(long rowCount) {
		return cacheRowCount += rowCount;
	}

	@Override
	public long addAndGetCacheDataSize(long dataSize) {
		return cacheDataSize += dataSize;
	}

	public void setCacheRowCount(long cacheRowCount) {
		this.cacheRowCount = cacheRowCount;
	}

	public void setHitCacheCount(long hitCacheCount) {
		this.hitCacheCount = hitCacheCount;
	}

	public void setMissCacheCount(long missCacheCount) {
		this.missCacheCount = missCacheCount;
	}

	public long[] toLongArray() {
		long[] array = new long[4];

		array[0] = this.cacheDataSize;
		array[1] = this.cacheRowCount;
		array[2] = this.hitCacheCount;
		array[3] = this.missCacheCount;

		return array;
	}
}
