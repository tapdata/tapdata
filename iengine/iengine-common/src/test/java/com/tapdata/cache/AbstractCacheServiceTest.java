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
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
