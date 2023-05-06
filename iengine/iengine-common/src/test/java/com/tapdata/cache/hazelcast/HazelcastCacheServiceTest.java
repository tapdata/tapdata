package com.tapdata.cache.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.tapdata.cache.CacheUtil;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import junit.framework.TestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class HazelcastCacheServiceTest extends TestCase {


	private HazelcastCacheService getHazelcastCacheService() {
		Config config = HazelcastUtil.getConfig("agentId");
		HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
		return new HazelcastCacheService(hazelcastInstance, null);
	}

	public void testRegisterCache() throws Throwable {

		HazelcastCacheService hazelcastCacheService = getHazelcastCacheService();

		String cacheName = "cacheName1";
		String cacheKeys = "k1,k2";
		Set<String> fields = new HashSet<String>() {{
			add("k1");
			add("v1");
		}};
		DataFlowCacheConfig dataFlowCacheConfig = new DataFlowCacheConfig(cacheKeys,
				cacheName,
				"all",
				1000L,
				1000L,
				10L,
				fields,
				null,
				null,
				"",
				null,
				Collections.singletonList("id")
		);

		hazelcastCacheService.registerCache(dataFlowCacheConfig);

		Map<String, Object> row = new HashMap<>();
		row.put("k1", "aa");
		row.put("k2", "bb");
		row.put("v1", "vv");
		row.put("v2", "ww");
		row.put("id", "11");

		final Object[] cacheKeyValues = CacheUtil.getKeyValues(Arrays.asList(cacheKeys.split(",")), row);
		String cacheKey = CacheUtil.cacheKey(cacheKeyValues);
		hazelcastCacheService.cacheRow(cacheName, cacheKey, Collections.singletonList(row));

		Map<String, Object> cacheValue = hazelcastCacheService.getCache(cacheName, "aa", "bb");

		System.out.println(cacheValue);


		hazelcastCacheService.destroy(cacheName);

	}

	public void testDestroy() {
	}
}
