package com.tapdata.cache;

import java.util.List;
import java.util.Map;

/**
 * cache store
 */
public interface ICacheStore {

	void cacheRow(String cacheName, String key, List<Map<String, Object>> rows) throws Throwable;

	void removeByKey(String cacheName, String cacheKey, String pkKey) throws Throwable;

	default void destroy() {

	}


}
