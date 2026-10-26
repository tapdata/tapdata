package com.tapdata.cache;

import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import io.tapdata.error.ShareCacheExCode_20;
import io.tapdata.exception.TapCodeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import org.voovan.tools.collection.CacheMap;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

public class AbstractCacheServiceTest {
    class TestCacheService extends AbstractCacheService{

        public TestCacheService(ClientMongoOperator clientMongoOperator, Map<String, String> cacheStatusMap) {
            super(clientMongoOperator, cacheStatusMap);
        }

        @Override
        protected Lock getCacheStatusLockInstance(String cacheName) {
            return null;

        }

        @Override
        protected ICacheGetter getCacheGetterInstance(String cacheName) {
            return null;
        }

        @Override
        protected ICacheStats getCacheStats(String cacheName) {
            return null;
        }

        @Override
        protected ICacheStore getCacheStore(String cacheName) {
            return null;
        }
    }
    @DisplayName("test encode cache name for exception")
    @Test
    void test1() {
        Map<String, String> cacheStatusMap = new HashMap<>();
        HttpClientMongoOperator httpClientMongoOperator = mock(HttpClientMongoOperator.class);
        TestCacheService testCacheService = new TestCacheService(httpClientMongoOperator, cacheStatusMap);
        Function<String, DataFlowCacheConfig> supplier = testCacheService.cacheConfigMap.getSupplier();
        try (MockedStatic<URLEncoder> urlEncoderMockedStatic = mockStatic(URLEncoder.class);) {
            urlEncoderMockedStatic.when(() -> {
                URLEncoder.encode("t1", "UTF-8");
            }).thenThrow(new UnsupportedEncodingException());
            TapCodeException tapCodeException = assertThrows(TapCodeException.class, () -> {
                supplier.apply("t1");
            });
            assertEquals(ShareCacheExCode_20.ENCODE_CACHE_NAME,tapCodeException.getCode());
        }
    }

    @DisplayName("destroying one cache must not block destroying another")
    @Test
    void destroyIsNotSerializedAcrossCacheNames() throws Exception {
        // destroy 里做的是物理销毁（集群范围、无超时），实例级 synchronized 会让不同 cache 的销毁互相排队，
        // 一个卡住的销毁就挡住本节点其它共享缓存任务的清理，那些任务的启动随之被无限期推迟（TAP-12865）
        CountDownLatch blockedEntered = new CountDownLatch(1);
        CountDownLatch releaseBlocked = new CountDownLatch(1);
        CountDownLatch otherDestroyed = new CountDownLatch(1);

        TestCacheService cacheService = new TestCacheService(mock(HttpClientMongoOperator.class), new ConcurrentHashMap<>()) {
            @Override
            protected ICacheStore getCacheStore(String cacheName) {
                return new ICacheStore() {
                    @Override
                    public void cacheRow(String name, String key, List<Map<String, Object>> rows) {
                    }

                    @Override
                    public void removeByKey(String name, String cacheKey, String pkKey) {
                    }

                    @Override
                    public void destroy() {
                        if ("blocked-cache".equals(cacheName)) {
                            blockedEntered.countDown();
                            try {
                                releaseBlocked.await(10, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        } else {
                            otherDestroyed.countDown();
                        }
                    }
                };
            }
        };

        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            workers.submit(() -> cacheService.destroy("blocked-cache"));
            assertTrue(blockedEntered.await(2, TimeUnit.SECONDS));

            workers.submit(() -> cacheService.destroy("other-cache"));
            assertTrue(otherDestroyed.await(2, TimeUnit.SECONDS),
                    "一个卡住的缓存销毁不得挡住另一个 cacheName 的销毁");
        } finally {
            releaseBlocked.countDown();
            workers.shutdownNow();
            assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /**
     * 回源连接器桩：记录关闭次数；可选在 getRows 中阻塞，模拟一次在途的回源查询
     */
    static class RecordingRowsGetter implements IDataSourceRowsGetter {
        final AtomicInteger closed = new AtomicInteger();
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release;

        RecordingRowsGetter(CountDownLatch release) {
            this.release = release;
        }

        @Override
        public List<Map<String, Object>> getRows(Object[] keys) {
            entered.countDown();
            if (release != null) {
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return Collections.emptyList();
        }

        @Override
        public void close() {
            closed.incrementAndGet();
        }
    }

    /**
     * 每次缓存未命中都会回源的 getter，回源连接器按需创建，与 ExternalStorageCacheGetter 的创建方式一致
     */
    static class LookupCacheGetter extends AbstractCacheGetter {
        private final Supplier<IDataSourceRowsGetter> rowsGetterFactory;

        LookupCacheGetter(Supplier<IDataSourceRowsGetter> rowsGetterFactory) {
            super(new DataFlowCacheConfig(), mock(ICacheStore.class), mock(ICacheStats.class), null);
            this.rowsGetterFactory = rowsGetterFactory;
        }

        @Override
        protected List<Map<String, Object>> getRecordList(String cacheName, Object... cacheKeys) {
            return null;
        }

        @Override
        protected Map<String, Object> getRecord(String cacheName, Object... cacheKeys) {
            return null;
        }

        @Override
        public IDataSourceRowsGetter getDataSourceRowsGetter() {
            if (dataSourceRowsGetter == null) {
                dataSourceRowsGetter = rowsGetterFactory.get();
            }
            return dataSourceRowsGetter;
        }
    }

    private TestCacheService lookupCacheService(Supplier<IDataSourceRowsGetter> rowsGetterFactory) {
        return new TestCacheService(mock(HttpClientMongoOperator.class), new ConcurrentHashMap<>()) {
            @Override
            protected ICacheGetter getCacheGetterInstance(String cacheName) {
                return new LookupCacheGetter(rowsGetterFactory);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static CacheMap<String, ICacheGetter> cacheGetterMap(AbstractCacheService cacheService) throws Exception {
        Field field = AbstractCacheService.class.getDeclaredField("cacheGetterMap");
        field.setAccessible(true);
        return (CacheMap<String, ICacheGetter>) field.get(cacheService);
    }

    private static void expireNow(CacheMap<String, ICacheGetter> getterMap, String cacheName) throws InterruptedException {
        getterMap.setTTL(cacheName, 1);
        TimeUnit.MILLISECONDS.sleep(1100);
        getterMap.checkAndDoExpire(cacheName);
    }

    @DisplayName("TAP-13150: expired cache getter must release its data source connector")
    @Test
    void expiredGetterReleasesDataSourceConnector() throws Throwable {
        // getter 每 5 分钟过期重建；旧 getter 的回源连接器（MongoDB 连接器持有 MongoClient）若不释放，
        // 每个周期泄漏一个客户端，长期运行后线程数撑满 vm.max_map_count 导致 JVM 崩溃
        List<RecordingRowsGetter> created = new CopyOnWriteArrayList<>();
        TestCacheService cacheService = lookupCacheService(() -> {
            RecordingRowsGetter rowsGetter = new RecordingRowsGetter(null);
            created.add(rowsGetter);
            return rowsGetter;
        });
        CacheMap<String, ICacheGetter> getterMap = cacheGetterMap(cacheService);

        ICacheGetter expired = getterMap.get("c1");
        expired.getAndSetCache("c1", true, "k1");
        assertEquals(1, created.size());

        expireNow(getterMap, "c1");

        assertEquals(1, created.get(0).closed.get(), "过期淘汰的 getter 必须释放其回源连接器");
        assertNotSame(expired, getterMap.get("c1"));
    }

    @DisplayName("TAP-13150: release of an expired getter waits for the in-flight lookup")
    @Test
    void expiredGetterReleaseWaitsForInFlightLookup() throws Throwable {
        CountDownLatch releaseLookup = new CountDownLatch(1);
        RecordingRowsGetter rowsGetter = new RecordingRowsGetter(releaseLookup);
        TestCacheService cacheService = lookupCacheService(() -> rowsGetter);
        CacheMap<String, ICacheGetter> getterMap = cacheGetterMap(cacheService);
        ICacheGetter expired = getterMap.get("c1");

        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> lookup = worker.submit(() -> {
                try {
                    return expired.getAndSetCache("c1", true, "k1");
                } catch (Throwable e) {
                    throw new IllegalStateException(e);
                }
            });
            assertTrue(rowsGetter.entered.await(2, TimeUnit.SECONDS));

            expireNow(getterMap, "c1");
            assertEquals(0, rowsGetter.closed.get(), "在途回源查询未结束时不得关闭连接器");

            releaseLookup.countDown();
            lookup.get(5, TimeUnit.SECONDS);
            assertEquals(1, rowsGetter.closed.get(), "在途查询结束后应关闭已淘汰 getter 的回源连接器");
        } finally {
            releaseLookup.countDown();
            worker.shutdownNow();
        }
    }
}
