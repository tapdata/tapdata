package com.tapdata.cache.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.tapdata.cache.CacheUtil;
import com.tapdata.constant.HazelcastUtil;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.cache.ICacheStore;
import junit.framework.TestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class HazelcastCacheServiceTest extends TestCase {
	private final List<HazelcastInstance> testInstances = new ArrayList<>();

	private static class TestableHazelcastCacheService extends HazelcastCacheService {
		private final AtomicReference<DataFlowCacheConfig> taskConfig;

		TestableHazelcastCacheService(HazelcastInstance hazelcastInstance) {
			this(hazelcastInstance, new AtomicReference<>());
		}

		TestableHazelcastCacheService(HazelcastInstance hazelcastInstance,
									 AtomicReference<DataFlowCacheConfig> taskConfig) {
			super(hazelcastInstance, null);
			this.taskConfig = taskConfig;
		}

		ICacheStore getStore(String cacheName) {
			return super.getCacheStore(cacheName);
		}

		@Override
		protected DataFlowCacheConfig loadConfigFromTm(String cacheName) {
			return taskConfig.get();
		}
	}


	private HazelcastCacheService getHazelcastCacheService() {
		Config config = HazelcastUtil.getConfig("cache-test-" + UUID.randomUUID());
		config.setClusterName("cache-test-" + UUID.randomUUID());
		HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);
		testInstances.add(hazelcastInstance);
		return new HazelcastCacheService(hazelcastInstance, null);
	}

	@Override
	protected void tearDown() throws Exception {
		for (HazelcastInstance instance : testInstances) {
			instance.shutdown();
		}
		testInstances.clear();
		super.tearDown();
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

	public void testDestroyAndReregisterCreatesStoreFromNewConfiguration() {
		HazelcastInstance hazelcastInstance = newHazelcastInstance("cache-reregister");
		TestableHazelcastCacheService cacheService = new TestableHazelcastCacheService(hazelcastInstance);
		String cacheName = "destroy-reregister-cache";
		DataFlowCacheConfig originalConfig = cacheConfig(cacheName, Collections.singletonList("oldPk"));
		cacheService.registerCache(originalConfig);
		ICacheStore originalStore = cacheService.getStore(cacheName);

		cacheService.destroy(cacheName);
		DataFlowCacheConfig updatedConfig = cacheConfig(cacheName, Collections.singletonList("newPk"));
		cacheService.registerCache(updatedConfig);
		ICacheStore updatedStore = cacheService.getStore(cacheName);

		assertNotSame(originalStore, updatedStore);
	}

	public void testOtherMemberImmediatelyRebuildsStoreAfterGenerationChange() throws Throwable {
		String clusterName = "cache-generation-" + UUID.randomUUID();
		HazelcastInstance firstMember = newHazelcastInstance(clusterName);
		HazelcastInstance secondMember = newHazelcastInstance(clusterName);
		String cacheName = "cross-member-cache";
		DataFlowCacheConfig originalConfig = cacheConfig(cacheName, Collections.singletonList("oldPk"));
		DataFlowCacheConfig updatedConfig = cacheConfig(cacheName, Collections.singletonList("newPk"));
		AtomicReference<DataFlowCacheConfig> firstTaskConfig = new AtomicReference<>(originalConfig);
		AtomicReference<DataFlowCacheConfig> secondTaskConfig = new AtomicReference<>(originalConfig);
		TestableHazelcastCacheService first = new TestableHazelcastCacheService(firstMember, firstTaskConfig);
		TestableHazelcastCacheService second = new TestableHazelcastCacheService(secondMember, secondTaskConfig);
		first.registerCache(originalConfig);
		second.registerCache(originalConfig);
		ICacheStore oldStore = second.getStore(cacheName);

		first.destroy(cacheName);
		firstTaskConfig.set(updatedConfig);
		secondTaskConfig.set(updatedConfig);
		first.registerCache(updatedConfig);

		ICacheStore rebuiltStore = second.getStore(cacheName);
		assertNotSame(oldStore, rebuiltStore);
		Map<String, Object> row = new HashMap<>();
		row.put("oldPk", "old");
		row.put("newPk", "new");
		rebuiltStore.cacheRow(cacheName, "partition", Collections.singletonList(row));
		Map<String, Map<String, Object>> data = ((HazelcastCacheStore) rebuiltStore).getDataMap();
		assertTrue(data.containsKey(CacheUtil.cacheKey("partition", "new")));
		assertFalse(data.containsKey(CacheUtil.cacheKey("partition", "old")));
	}

	public void testOtherMemberRebuildsStoreAfterImmediateGenerationChange() throws Throwable {
		String cluster = "cache-generation-" + UUID.randomUUID();
		HazelcastInstance firstMember = newHazelcastInstance(cluster);
		HazelcastInstance secondMember = newHazelcastInstance(cluster);
		String cacheName = "two-member-cache";
		DataFlowCacheConfig originalConfig = cacheConfig(cacheName, Collections.singletonList("oldPk"));
		DataFlowCacheConfig updatedConfig = cacheConfig(cacheName, Collections.singletonList("newPk"));
		AtomicReference<DataFlowCacheConfig> firstTaskConfig = new AtomicReference<>(originalConfig);
		AtomicReference<DataFlowCacheConfig> secondTaskConfig = new AtomicReference<>(updatedConfig);
		TestableHazelcastCacheService firstService = new TestableHazelcastCacheService(firstMember, firstTaskConfig);
		TestableHazelcastCacheService secondService = new TestableHazelcastCacheService(secondMember, secondTaskConfig);
		firstService.registerCache(originalConfig);
		secondService.registerCache(originalConfig);
		ICacheStore oldStore = secondService.getStore(cacheName);

		firstService.destroy(cacheName);
		firstTaskConfig.set(updatedConfig);
		firstService.registerCache(updatedConfig);

		ICacheStore newStore = secondService.getStore(cacheName);
		assertNotSame(oldStore, newStore);
		Map<String, Object> row = new HashMap<>();
		row.put("oldPk", "old");
		row.put("newPk", "new");
		newStore.cacheRow(cacheName, "partition", Collections.singletonList(row));
		HazelcastCacheStore hazelcastStore = (HazelcastCacheStore) newStore;
		assertTrue(hazelcastStore.getDataMap().containsKey(CacheUtil.cacheKey("partition", "new")));
		assertFalse(hazelcastStore.getDataMap().containsKey(CacheUtil.cacheKey("partition", "old")));
	}

	private HazelcastInstance newHazelcastInstance(String clusterName) {
		Config config = HazelcastUtil.getConfig("cache-test-" + UUID.randomUUID());
		config.setClusterName(clusterName);
		config.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
		HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
		testInstances.add(instance);
		return instance;
	}

	private DataFlowCacheConfig cacheConfig(String cacheName, java.util.List<String> primaryKeys) {
		return new DataFlowCacheConfig("k1,k2", cacheName, "all", 1000L, 1000L, 10L,
				new HashSet<>(Arrays.asList("k1", "v1")), null, null, "", null, primaryKeys);
	}
}
