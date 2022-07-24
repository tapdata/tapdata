package com.tapdata.cache;

import com.tapdata.constant.MapUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MemoryCacheUtil {

	public final static String CACHE_KEY_SEPERATE = "-";
	public final static Logger logger = LogManager.getLogger(MemoryCacheUtil.class);


	public static String cacheDataKey(String cacheName) {
		return "cache" + CACHE_KEY_SEPERATE + cacheName + CACHE_KEY_SEPERATE + "data";
	}

	public static String cacheIndexKey(String cacheName) {
		return "cache" + CACHE_KEY_SEPERATE + cacheName + CACHE_KEY_SEPERATE + "index";
	}

	public static String cacheKey(String pre, Object... cacheKeys) {
		StringBuilder sb = new StringBuilder(pre);
		sb.append(CACHE_KEY_SEPERATE);
		if (cacheKeys != null) {
			for (Object cacheKey : cacheKeys) {
				sb.append(cacheKey).append(CACHE_KEY_SEPERATE);
			}
		}
		return sb.toString();
	}

	public static String cacheKey(Object... cacheKeys) {
		StringBuilder sb = new StringBuilder();
		if (cacheKeys != null) {
			for (Object cacheKey : cacheKeys) {
				sb.append(cacheKey).append(CACHE_KEY_SEPERATE);
			}
		}

		return sb.toString();
	}

	public static Object[] getKeyValues(List<String> keys, Map<String, Object> row) {
		if (CollectionUtils.isEmpty(keys)) {
			return null;
		}

		Object[] keyValues = new Object[keys.size()];
		for (int i = 0; i < keys.size(); i++) {
			final String key = keys.get(i);
			if (MapUtil.containsKey(row, key)) {
				keyValues[i] = MapUtil.getValueByKey(row, key);
			} else {
				keyValues[i] = null;
			}
		}
		return keyValues;
	}

	public static Map<String, Object> returnCacheRow(Map<String, Object> result) {

		if (MapUtils.isNotEmpty(result)) {
			Map<String, Object> newMap = new HashMap<>();
			MapUtil.copyToNewMap(result, newMap);
			result = newMap;
		}

		return result;
	}

	public static long byteToMB(long bytes) {
		return bytes / 1024 / 1024;
	}

	public static long byteToKB(long bytes) {
		return bytes / 1024;
	}
}
