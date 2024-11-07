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
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
}
