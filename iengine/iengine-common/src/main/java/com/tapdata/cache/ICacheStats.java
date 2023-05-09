package com.tapdata.cache;

/**
 * cache stats
 */
public interface ICacheStats {


	long getCacheDataSize();

	void setCacheDataSize(long dataSize);

	long getCacheRowCount();

	long getHitCacheCount();

	long getMissCacheCount();

	void hitRateStats(String cacheName, boolean hitCache);

	double getHitRate();

	long addAndGetCacheRowCount(long rowCount);

	long addAndGetCacheDataSize(long dataSize);

	long getLastLogTs();
}
