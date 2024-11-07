package com.tapdata.cache;

import com.tapdata.mongo.ClientMongoOperator;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.locks.Lock;

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
    @Test
    void test1(){

    }
}
