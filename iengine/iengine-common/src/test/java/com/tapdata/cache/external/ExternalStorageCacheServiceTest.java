package com.tapdata.cache.external;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.tapdata.cache.CacheUtil;
import com.tapdata.cache.ICacheStore;
import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.construct.constructImpl.ConstructIMap;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ExternalStorageCacheServiceTest {
    private static final String GENERATION_MAP_NAME = CacheUtil.CACHE_NAME_PREFIX + "generation";

    private static class TestStore implements ICacheStore {
        final DataFlowCacheConfig config;
        final AtomicInteger destroyCalls = new AtomicInteger();
        boolean failDestroy;
        private CountDownLatch destroyStarted;
        private CountDownLatch releaseDestroy;

        TestStore(DataFlowCacheConfig config) {
            this.config = config;
        }

        @Override
        public void cacheRow(String cacheName, String key, List<Map<String, Object>> rows) {
        }

        @Override
        public void removeByKey(String cacheName, String cacheKey, String pkKey) {
        }

        @Override
        public void destroy() {
            destroyCalls.incrementAndGet();
            if (destroyStarted != null) {
                destroyStarted.countDown();
                try {
                    releaseDestroy.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            if (failDestroy) throw new IllegalStateException("simulated destroy failure");
        }

        void blockDestroy(CountDownLatch entered, CountDownLatch release) {
            destroyStarted = entered;
            releaseDestroy = release;
        }
    }

    private static class TestService extends ExternalStorageCacheService {
        private final AtomicReference<DataFlowCacheConfig> remoteConfig;
        private final List<TestStore> stores;

        TestService(HazelcastInstance hazelcast, AtomicReference<DataFlowCacheConfig> remoteConfig,
                    List<TestStore> stores) {
            super(hazelcast, mock(ClientMongoOperator.class));
            this.remoteConfig = remoteConfig;
            this.stores = stores;
        }

        @Override
        protected DataFlowCacheConfig loadConfigFromTm(String cacheName) {
            return remoteConfig.get();
        }

        @Override
        protected ICacheStore createCacheStore(DataFlowCacheConfig config) {
            TestStore store = new TestStore(config);
            stores.add(store);
            return store;
        }

        ICacheStore store(String cacheName) {
            return super.getCacheStore(cacheName);
        }

        boolean hasStore(String cacheName) {
            return getCacheStoreMap().containsKey(cacheName);
        }

        void seedStore(String cacheName, ICacheStore store) {
            getCacheStoreMap().put(cacheName, store);
        }
    }

    private static class ProductionStoreService extends ExternalStorageCacheService {
        private final AtomicReference<DataFlowCacheConfig> remoteConfig;
        private final List<ConstructIMap<Map<String, Map<String, Object>>>> dataMaps;

        ProductionStoreService(HazelcastInstance hazelcast, AtomicReference<DataFlowCacheConfig> remoteConfig,
                               List<ConstructIMap<Map<String, Map<String, Object>>>> dataMaps) {
            super(hazelcast, mock(ClientMongoOperator.class));
            this.remoteConfig = remoteConfig;
            this.dataMaps = dataMaps;
        }

        @Override
        protected DataFlowCacheConfig loadConfigFromTm(String cacheName) {
            return remoteConfig.get();
        }

        @Override
        protected ICacheStore createCacheStore(DataFlowCacheConfig config) {
            @SuppressWarnings("unchecked")
            ConstructIMap<Map<String, Map<String, Object>>> dataMap = mock(ConstructIMap.class);
            try {
                when(dataMap.exists(anyString())).thenReturn(false);
                when(dataMap.insert(anyString(), any())).thenReturn(1);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            dataMaps.add(dataMap);
            return new ExternalStorageCacheStore(config, dataMap);
        }

        ICacheStore store(String cacheName) {
            return super.getCacheStore(cacheName);
        }
    }

    private static class SharedHazelcast {
        final ConcurrentHashMap<String, String> values = new ConcurrentHashMap<>();
        final IMap<String, String> map = mock(IMap.class);
        final HazelcastInstance instance = mock(HazelcastInstance.class);

        @SuppressWarnings("unchecked")
        SharedHazelcast() {
            when(instance.<String, String>getMap(GENERATION_MAP_NAME)).thenReturn(map);
            when(map.get(anyString())).thenAnswer(invocation -> values.get(invocation.getArgument(0)));
            doAnswer(invocation -> values.put(invocation.getArgument(0), invocation.getArgument(1)))
                    .when(map).put(anyString(), anyString());
        }
    }

    @Test
    void destroyWithMissingConfigStillEvictsAndDestroysCachedStore() {
        SharedHazelcast hazelcast = new SharedHazelcast();
        DataFlowCacheConfig config = config("missing-config", "oldPk");
        AtomicReference<DataFlowCacheConfig> remote = new AtomicReference<>(null);
        List<TestStore> stores = new java.util.concurrent.CopyOnWriteArrayList<>();
        TestService service = new TestService(hazelcast.instance, remote, stores);
        TestStore store = new TestStore(config);
        service.seedStore("missing-config", store);

        service.destroy("missing-config");

        assertEquals(1, store.destroyCalls.get());
        assertTrue(!service.hasStore("missing-config"));
    }

    @Test
    void destroyFailureStillClearsLocalStoreAndAllowsFreshRegistration() {
        SharedHazelcast hazelcast = new SharedHazelcast();
        DataFlowCacheConfig configA = config("destroy-failure", "oldPk");
        AtomicReference<DataFlowCacheConfig> remote = new AtomicReference<>(configA);
        List<TestStore> stores = new java.util.concurrent.CopyOnWriteArrayList<>();
        TestService service = new TestService(hazelcast.instance, remote, stores);
        service.registerCache(configA);
        TestStore oldStore = (TestStore) service.store("destroy-failure");
        oldStore.failDestroy = true;

        service.destroy("destroy-failure");

        assertTrue(!service.hasStore("destroy-failure"));
        DataFlowCacheConfig configB = config("destroy-failure", "newPk");
        remote.set(configB);
        service.registerCache(configB);
        assertNotSame(oldStore, service.store("destroy-failure"));
        assertSame(configB, stores.get(stores.size() - 1).config);
    }

    @Test
    void otherMemberRebuildsProductionServiceStoreFromNewConfigGeneration() {
        SharedHazelcast hazelcast = new SharedHazelcast();
        String cacheName = "cross-member";
        DataFlowCacheConfig configA = config(cacheName, "oldPk");
        DataFlowCacheConfig configB = config(cacheName, "newPk");
        AtomicReference<DataFlowCacheConfig> taskA = new AtomicReference<>(configA);
        AtomicReference<DataFlowCacheConfig> taskB = new AtomicReference<>(configA);
        List<TestStore> storesA = new java.util.concurrent.CopyOnWriteArrayList<>();
        List<TestStore> storesB = new java.util.concurrent.CopyOnWriteArrayList<>();
        TestService first = new TestService(hazelcast.instance, taskA, storesA);
        TestService second = new TestService(hazelcast.instance, taskB, storesB);
        first.registerCache(configA);
        second.registerCache(configA);
        ICacheStore oldStoreOnSecond = second.store(cacheName);

        first.destroy(cacheName);
        taskA.set(configB);
        first.registerCache(configB);
        taskB.set(configB);

        ICacheStore refreshedStoreOnSecond = second.store(cacheName);

        assertNotSame(oldStoreOnSecond, refreshedStoreOnSecond);
        assertSame(configB, storesB.get(storesB.size() - 1).config);
    }

    @Test
    void productionExternalStoreUsesNewPrimaryKeyAfterSameNameRegistration() throws Throwable {
        SharedHazelcast hazelcast = new SharedHazelcast();
        String cacheName = "external-same-name-reregister";
        DataFlowCacheConfig configA = config(cacheName, "oldPk");
        DataFlowCacheConfig configB = config(cacheName, "newPk");
        AtomicReference<DataFlowCacheConfig> remote = new AtomicReference<>(configA);
        List<ConstructIMap<Map<String, Map<String, Object>>>> maps = new java.util.concurrent.CopyOnWriteArrayList<>();
        ProductionStoreService service = new ProductionStoreService(hazelcast.instance, remote, maps);
        service.registerCache(configA);
        ICacheStore oldStore = service.store(cacheName);

        remote.set(configB);
        service.registerCache(configB);
        ICacheStore newStore = service.store(cacheName);
        assertNotSame(oldStore, newStore);

        Map<String, Object> row = new java.util.HashMap<>();
        row.put("oldPk", "old-value");
        row.put("newPk", "new-value");
        newStore.cacheRow(cacheName, "partition", Collections.singletonList(row));
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, Map<String, Object>>> captured = ArgumentCaptor.forClass(Map.class);
        verify(maps.get(1)).insert(eq("partition"), captured.capture());
        assertTrue(captured.getValue().containsKey(CacheUtil.cacheKey("new-value")));
        assertTrue(!captured.getValue().containsKey(CacheUtil.cacheKey("old-value")));
    }

    @Test
    void readerCannotObtainStoreWhileDestroyIsInFlight() throws Exception {
        SharedHazelcast hazelcast = new SharedHazelcast();
        DataFlowCacheConfig config = config("in-flight-destroy", "pk");
        AtomicReference<DataFlowCacheConfig> remote = new AtomicReference<>(config);
        List<TestStore> stores = new java.util.concurrent.CopyOnWriteArrayList<>();
        TestService service = new TestService(hazelcast.instance, remote, stores);
        service.registerCache(config);
        TestStore store = (TestStore) service.store("in-flight-destroy");
        CountDownLatch destroyStarted = new CountDownLatch(1);
        CountDownLatch releaseDestroy = new CountDownLatch(1);
        store.blockDestroy(destroyStarted, releaseDestroy);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            var destroy = executor.submit(() -> service.destroy("in-flight-destroy"));
            assertTrue(destroyStarted.await(2, TimeUnit.SECONDS));
            Future<ICacheStore> read = executor.submit(() -> service.store("in-flight-destroy"));
            assertThrows(TimeoutException.class, () -> read.get(200, TimeUnit.MILLISECONDS));
            releaseDestroy.countDown();
            destroy.get(2, TimeUnit.SECONDS);
            ExecutionException failedRead = assertThrows(ExecutionException.class, () -> read.get(2, TimeUnit.SECONDS));
            assertTrue(failedRead.getCause() instanceof IllegalStateException);
        } finally {
            releaseDestroy.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static DataFlowCacheConfig config(String cacheName, String primaryKey) {
        DataFlowCacheConfig config = new DataFlowCacheConfig("key", cacheName, "all", 1000L, 10L, 10L,
                Set.of("key", primaryKey), null, null, "table", null, Collections.singletonList(primaryKey));
        return config;
    }
}
