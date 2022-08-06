package com.tapdata.tm.config;

import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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

}
