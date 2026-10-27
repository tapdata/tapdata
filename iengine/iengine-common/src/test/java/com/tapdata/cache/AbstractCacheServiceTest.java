package com.tapdata.cache;

import com.tapdata.entity.dataflow.DataFlowCacheConfig;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import io.tapdata.error.ShareCacheExCode_20;
import io.tapdata.exception.TapCodeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

public class AbstractCacheServiceTest {
    class TestCacheService extends AbstractCacheService{
        final AtomicLong now = new AtomicLong(1_000_000L);

        public TestCacheService(ClientMongoOperator clientMongoOperator, Map<String, String> cacheStatusMap) {
            super(clientMongoOperator, cacheStatusMap);
        }

        @Override
        protected long currentTimeMillis() {
            return now.get();
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

    private static ICacheGetter cacheGetter(AbstractCacheService cacheService, String cacheName) throws Exception {
        Method method = AbstractCacheService.class.getDeclaredMethod("getCacheGetter", String.class);
        method.setAccessible(true);
        return (ICacheGetter) method.invoke(cacheService, cacheName);
    }

    /**
     * 让 getter 过期：下一次访问时重建，替换下来的旧 getter 被释放
     */
    private static void expire(TestCacheService cacheService) {
        cacheService.now.addAndGet(AbstractCacheService.CACHE_GETTER_EXPIRE_MS);
    }

    /**
     * 让 getter 过期并访问一次，触发替换与旧 getter 的释放
     */
    private static void expireNow(TestCacheService cacheService, String cacheName) throws Exception {
        expire(cacheService);
        cacheGetter(cacheService, cacheName);
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
        registerConfig(cacheService, "c1");

        ICacheGetter expired = cacheGetter(cacheService, "c1");
        expired.getAndSetCache("c1", true, "k1");
        assertEquals(1, created.size());

        expireNow(cacheService, "c1");

        assertTrue(waitFor(() -> created.get(0).closed.get() == 1), "过期淘汰的 getter 必须释放其回源连接器");
        assertNotSame(expired, cacheGetter(cacheService, "c1"));
    }

    @DisplayName("TAP-13150: release of an expired getter waits for the in-flight lookup")
    @Test
    void expiredGetterReleaseWaitsForInFlightLookup() throws Throwable {
        CountDownLatch releaseLookup = new CountDownLatch(1);
        RecordingRowsGetter rowsGetter = new RecordingRowsGetter(releaseLookup);
        TestCacheService cacheService = lookupCacheService(() -> rowsGetter);
        registerConfig(cacheService, "c1");
        ICacheGetter expired = cacheGetter(cacheService, "c1");

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

            expireNow(cacheService, "c1");
            TimeUnit.MILLISECONDS.sleep(200);
            assertEquals(0, rowsGetter.closed.get(), "在途回源查询未结束时不得关闭连接器");

            releaseLookup.countDown();
            lookup.get(5, TimeUnit.SECONDS);
            assertTrue(waitFor(() -> rowsGetter.closed.get() == 1), "在途查询结束后应关闭已淘汰 getter 的回源连接器");
        } finally {
            releaseLookup.countDown();
            worker.shutdownNow();
        }
    }

    // ---- PR #3561 审查返工 ----

    private static void registerConfig(AbstractCacheService cacheService, String cacheName) {
        cacheService.cacheConfigMap.put(cacheName, new DataFlowCacheConfig());
    }

    @DisplayName("TAP-13150 review: concurrent expiry builds one replacement and never orphans a getter")
    @Test
    void concurrentExpiryDoesNotOrphanGetter() throws Throwable {
        // 两个线程同时读到同一个过期 getter：被覆盖的 getter 不再在映射里，也不会收到淘汰回调，
        // 它回源创建的连接器永远不会关闭
        List<RecordingRowsGetter> connectors = new CopyOnWriteArrayList<>();
        List<LookupCacheGetter> getters = new CopyOnWriteArrayList<>();
        CountDownLatch replacing = new CountDownLatch(2);
        TestCacheService cacheService = new TestCacheService(mock(HttpClientMongoOperator.class), new ConcurrentHashMap<>()) {
            @Override
            protected ICacheGetter getCacheGetterInstance(String cacheName) {
                LookupCacheGetter getter = new LookupCacheGetter(() -> {
                    RecordingRowsGetter rowsGetter = new RecordingRowsGetter(null);
                    connectors.add(rowsGetter);
                    return rowsGetter;
                });
                getters.add(getter);
                if (getters.size() > 1) {
                    // 拉长替换过程，给另一个线程同时进入的机会
                    replacing.countDown();
                    try {
                        replacing.await(500, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return getter;
            }
        };
        registerConfig(cacheService, "c1");
        cacheService.getAndSetCache("c1", true, "k1");
        expire(cacheService);

        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Callable<Object> lookup = () -> {
                try {
                    return cacheService.getAndSetCache("c1", true, "k1");
                } catch (Throwable e) {
                    throw new IllegalStateException(e);
                }
            };
            Future<Object> first = workers.submit(lookup);
            Future<Object> second = workers.submit(lookup);
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
        } finally {
            workers.shutdownNow();
        }
        assertTrue(waitFor(() -> getters.get(0).dataSourceRowsGetter == null), "替换下来的旧 getter 必须被释放");

        assertEquals(2, getters.size(), "过期后只能创建一个替换 getter");
        ICacheGetter current = cacheGetter(cacheService, "c1");
        for (LookupCacheGetter getter : getters) {
            if (getter != current && getter.dataSourceRowsGetter != null) {
                throw new AssertionError("不在映射中的 getter 仍持有未关闭的回源连接器");
            }
        }
    }

    @DisplayName("TAP-13150 review: lookup after close releases the connector it creates")
    @Test
    void lookupAfterCloseReleasesConnector() throws Throwable {
        // destroy 先移除再 close；移除前拿到 getter 的线程在 close 之后回源，会新建一个无人关闭的连接器
        List<RecordingRowsGetter> connectors = new CopyOnWriteArrayList<>();
        LookupCacheGetter getter = new LookupCacheGetter(() -> {
            RecordingRowsGetter rowsGetter = new RecordingRowsGetter(null);
            connectors.add(rowsGetter);
            return rowsGetter;
        });
        getter.close();

        getter.getAndSetCache("c1", true, "k1");

        assertEquals(1, connectors.size());
        assertEquals(1, connectors.get(0).closed.get(), "close 之后回源创建的连接器必须在用完后关闭");
    }

    @DisplayName("TAP-13150 review: failure while releasing an expired getter does not break cache reads")
    @Test
    void releaseFailureDoesNotBreakCacheRead() throws Throwable {
        TestCacheService cacheService = new TestCacheService(mock(HttpClientMongoOperator.class), new ConcurrentHashMap<>()) {
            @Override
            protected ICacheGetter getCacheGetterInstance(String cacheName) {
                return new LookupCacheGetter(() -> new RecordingRowsGetter(null)) {
                    @Override
                    public void releaseDataSource() {
                        throw new IllegalStateException("connector stop failed");
                    }
                };
            }
        };
        registerConfig(cacheService, "c1");
        cacheService.getAndSetCache("c1", true, "k1");
        expire(cacheService);

        assertDoesNotThrow(() -> cacheService.getAndSetCache("c1", true, "k1"), "释放旧连接器失败不能让读缓存报错");
    }

    @DisplayName("TAP-13150 review: expired getter is released off the reading thread")
    @Test
    void expiredGetterReleasedOffReadingThread() throws Throwable {
        // 释放会执行连接器 STOP，可能阻塞；不能在读缓存的线程或全局定时器线程上同步执行
        AtomicReference<Thread> releasedOn = new AtomicReference<>();
        CountDownLatch released = new CountDownLatch(1);
        TestCacheService cacheService = new TestCacheService(mock(HttpClientMongoOperator.class), new ConcurrentHashMap<>()) {
            @Override
            protected ICacheGetter getCacheGetterInstance(String cacheName) {
                return new LookupCacheGetter(() -> new RecordingRowsGetter(null)) {
                    @Override
                    public void releaseDataSource() {
                        releasedOn.set(Thread.currentThread());
                        released.countDown();
                    }
                };
            }
        };
        registerConfig(cacheService, "c1");
        cacheService.getAndSetCache("c1", true, "k1");
        expire(cacheService);

        cacheService.getAndSetCache("c1", true, "k1");

        assertTrue(released.await(2, TimeUnit.SECONDS));
        assertNotSame(Thread.currentThread(), releasedOn.get(), "释放不能在读缓存的线程上执行");
    }

    @DisplayName("TAP-13150 review: releasing a getter never closes a connector that a lookup is using")
    @Test
    void releaseNeverClosesConnectorInUse() throws Throwable {
        // T1 结束最后一次在途查询并准备关闭连接器时，T2 开始回源并取到同一个连接器
        CountDownLatch firstLookup = new CountDownLatch(1);
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch proceedClose = new CountDownLatch(1);
        CountDownLatch secondEntered = new CountDownLatch(1);
        AtomicInteger closedWhileInUse = new AtomicInteger();
        List<RecordingRowsGetter> connectors = new CopyOnWriteArrayList<>();
        LookupCacheGetter getter = new LookupCacheGetter(() -> {
            RecordingRowsGetter rowsGetter = new RecordingRowsGetter(null) {
                @Override
                public List<Map<String, Object>> getRows(Object[] keys) {
                    if ("first".equals(Thread.currentThread().getName())) {
                        try {
                            firstLookup.await(5, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    } else {
                        secondEntered.countDown();
                        try {
                            // 使用期间放行关闭，观察连接器是否被关掉
                            proceedClose.countDown();
                            TimeUnit.MILLISECONDS.sleep(300);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        if (closed.get() > 0) {
                            closedWhileInUse.incrementAndGet();
                        }
                    }
                    return Collections.emptyList();
                }
            };
            connectors.add(rowsGetter);
            return rowsGetter;
        }) {
            @Override
            protected void closeDataSourceRowsGetter() {
                if ("first".equals(Thread.currentThread().getName())) {
                    closing.countDown();
                    try {
                        proceedClose.await(2, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                super.closeDataSourceRowsGetter();
            }
        };

        Thread first = new Thread(() -> {
            try {
                getter.getAndSetCache("c1", true, "k1");
            } catch (Throwable e) {
                throw new IllegalStateException(e);
            }
        }, "first");
        first.start();
        assertTrue(waitFor(() -> !connectors.isEmpty()));
        getter.releaseDataSource();
        firstLookup.countDown();
        assertTrue(closing.await(2, TimeUnit.SECONDS));

        Thread second = new Thread(() -> {
            try {
                getter.getAndSetCache("c1", true, "k1");
            } catch (Throwable e) {
                throw new IllegalStateException(e);
            }
        }, "second");
        second.start();
        secondEntered.await(1, TimeUnit.SECONDS);
        proceedClose.countDown();
        first.join(5000);
        second.join(5000);

        assertEquals(0, closedWhileInUse.get(), "不能关闭正在被回源查询使用的连接器");
    }

    private static boolean waitFor(java.util.function.BooleanSupplier condition) throws InterruptedException {
        for (int i = 0; i < 200 && !condition.getAsBoolean(); i++) {
            TimeUnit.MILLISECONDS.sleep(10);
        }
        return condition.getAsBoolean();
    }
}
