package io.tapdata.observable.logging.debug;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import org.ehcache.Cache;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;


/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/11/22 14:11
 */
public class DataCacheFactoryTest {

    @Test
    void testGetDataCacheDirectory() {

        ConfigurationCenter configurationCenter = new ConfigurationCenter();

        try (MockedStatic<BeanUtil> mockBeanUtil = mockStatic(BeanUtil.class)) {

            mockBeanUtil.when(() -> BeanUtil.getBean(eq(ConfigurationCenter.class))).thenReturn(null);

            DataCacheFactory dataCacheFactory = DataCacheFactory.getInstance();
            String dir = dataCacheFactory.getDataCacheDirectory();
            Assertions.assertEquals("./cache", dir);

            mockBeanUtil.when(() -> BeanUtil.getBean(eq(ConfigurationCenter.class))).thenReturn(configurationCenter);
            dir = dataCacheFactory.getDataCacheDirectory();
            Assertions.assertEquals("./cache", dir);

            setEnv("TAPDATA_WORK_DIR", "/test_1");
            dir = dataCacheFactory.getDataCacheDirectory();
            Assertions.assertEquals("/test_1/cache", dir);

            configurationCenter.putConfig(ConfigurationCenter.WORK_DIR, "/test");
            dir = dataCacheFactory.getDataCacheDirectory();
            Assertions.assertEquals("/test/cache", dir);
        }
    }

    @Test
    void testGetDataCache() {
        DataCacheFactory dataCacheFactory = DataCacheFactory.getInstance();
        DataCache dataCache = dataCacheFactory.getDataCache("testId");
        Assertions.assertNotNull(dataCache);

        DataCache dataCache1 = dataCacheFactory.getDataCache("testId");
        Assertions.assertEquals(dataCache, dataCache1);

        dataCacheFactory.removeDataCache("test1");
        dataCache1 = dataCacheFactory.getDataCache("testId");
        Assertions.assertNotNull(dataCache1);
        Assertions.assertEquals(dataCache, dataCache1);

        dataCacheFactory.removeDataCache("testId");
        dataCache1 = dataCacheFactory.getDataCache("testId");
        Assertions.assertNotNull(dataCache1);
        Assertions.assertNotEquals(dataCache, dataCache1);
    }

    @Test
    void testDataCachePut() {
        Cache<String, DataCache.CacheItem> cache = mock(Cache.class);
        List<DataCache.CacheItem> cacheData = new ArrayList<>();
        doAnswer(answer -> {
            DataCache.CacheItem item = answer.getArgument(1);
            cacheData.add(item);
            return null;
        }).when(cache).put(anyString(), any());
        doAnswer(answer -> {
            Consumer consumer = answer.getArgument(0);
            cacheData.forEach(t -> {
                consumer.accept(new Cache.Entry<String, DataCache.CacheItem>(){

                    @Override
                    public String getKey() {
                        return t.getId();
                    }

                    @Override
                    public DataCache.CacheItem getValue() {
                        return t;
                    }
                });
            });
            return null;
        }).when(cache).forEach(any());
        DataCache dataCache = new DataCache("taskId", 10L, cache);

        Assertions.assertEquals("taskId", dataCache.getTaskId());
        Assertions.assertEquals(10, dataCache.getMaxTotalEntries());
        dataCache.setTaskId("taskId1");
        dataCache.setMaxTotalEntries(20L);
        Assertions.assertEquals(20, dataCache.getMaxTotalEntries());
        dataCache.setMaxTotalEntries(10L);

        Map<String, Object> status = dataCache.getStatus();
        Assertions.assertNotNull(status);
        Assertions.assertEquals(0L, status.get("cacheCount"));

        for (int i = 0; i < 20; i++) {
            dataCache.put(MonitoringLogsDto.builder().logTag("catchData").logTag("eid=e" + i).build());
        }
        verify(cache, times(10)).put(anyString(), any());
        status = dataCache.getStatus();
        Assertions.assertNotNull(status);
        Assertions.assertEquals(10L, status.get("cacheCount"));

        for (int i = 0; i < 30; i++) {
            dataCache.put(new MonitoringLogsDto());
        }
        verify(cache, times(10)).put(anyString(), any());

        status = dataCache.getStatus();
        Assertions.assertNotNull(status);
        Assertions.assertEquals(10L, status.get("cacheCount"));
    }

    public static void setEnv(String key, String value) {
        try {
            Map<String, String> env = System.getenv();
            Class<?> cl = env.getClass();
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Map<String, String> writableEnv = (Map<String, String>) field.get(env);
            writableEnv.put(key, value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set environment variable", e);
        }
    }

}
