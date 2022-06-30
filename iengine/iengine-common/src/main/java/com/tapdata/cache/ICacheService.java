package com.tapdata.cache;

public interface ICacheService extends ICacheStore, ICacheGetter, ICacheConfigurator {

	void updateCacheStatus(String cacheName, String status);
}
