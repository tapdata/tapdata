package com.tapdata.cache.mapdb;

import com.tapdata.cache.AbstractCacheStats;

public class MapdbCacheStats extends AbstractCacheStats {
	@Override
	protected void hitStats(String cacheName, boolean hitCache) {

	}

	@Override
	public long getCacheDataSize() {
		return 0;
	}

	@Override
	public void setCacheDataSize(long dataSize) {

	}

	@Override
	public long getCacheRowCount() {
		return 0;
	}

	@Override
	public long getHitCacheCount() {
		return 0;
	}

	@Override
	public long getMissCacheCount() {
		return 0;
	}

	@Override
	public double getHitRate() {
		return 0;
	}

	@Override
	public long addAndGetCacheRowCount(long rowCount) {
		return 0;
	}

	@Override
	public long addAndGetCacheDataSize(long dataSize) {
		return 0;
	}
}
