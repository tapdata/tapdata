package com.tapdata.tm.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.concurrent.TimeUnit;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/1/28 上午9:14
 * @description
 */
@Configuration
@EnableCaching
public class CacheConfig {
    @Bean("memoryCache")
    public CacheManager memoryCacheManager() {
        return new ConcurrentMapCacheManager();
    }

    @Bean("caffeineCache")
    @Primary
    public CaffeineCacheManager cacheManager() {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager();
        cacheManager.setCaffeine(Caffeine.newBuilder()
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .initialCapacity(100)
                .maximumSize(500)
        );
        return cacheManager;
    }


}
