package io.tapdata.observable.logging.debug;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import org.ehcache.Cache;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
            Assertions.assertEquals("./debug_data", dir);

            mockBeanUtil.when(() -> BeanUtil.getBean(eq(ConfigurationCenter.class))).thenReturn(configurationCenter);
            dir = dataCacheFactory.getDataCacheDirectory();
            Assertions.assertEquals("./debug_data", dir);

            setEnv("TAPDATA_WORK_DIR", "/test_1");
            dir = dataCacheFactory.getDataCacheDirectory();
            Assertions.assertEquals("/test_1/debug_data", dir);

            configurationCenter.putConfig(ConfigurationCenter.WORK_DIR, "/test");
            dir = dataCacheFactory.getDataCacheDirectory();
            Assertions.assertEquals("/test/debug_data", dir);
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
        Cache<String, MonitoringLogsDto> cache = mock(Cache.class);
        DataCache dataCache = new DataCache("taskId", 10, cache);

        Assertions.assertEquals("taskId", dataCache.getTaskId());
        Assertions.assertEquals(10, dataCache.getMaxTotalEntries());
        dataCache.setTaskId("taskId1");
        dataCache.setMaxTotalEntries(20);
        Assertions.assertEquals(20, dataCache.getMaxTotalEntries());
        dataCache.setMaxTotalEntries(10);

        Map<String, Object> status = dataCache.getStatus();
        Assertions.assertNotNull(status);
        Assertions.assertEquals(0, status.get("counter"));

        for (int i = 0; i < 20; i++) {
            dataCache.put(new MonitoringLogsDto());
        }
        verify(cache, times(10)).put(anyString(), any());
        status = dataCache.getStatus();
        Assertions.assertNotNull(status);
        Assertions.assertEquals(20, status.get("counter"));

        when(cache.iterator()).thenReturn(new Iterator<Cache.Entry<String, MonitoringLogsDto>>() {
            int count = 5;
            @Override
            public boolean hasNext() {
                return count-- > 0;
            }

            @Override
            public Cache.Entry<String, MonitoringLogsDto> next() {
                return new Cache.Entry<String, MonitoringLogsDto>() {
                    @Override
                    public String getKey() {
                        return "t";
                    }

                    @Override
                    public MonitoringLogsDto getValue() {
                        return null;
                    }
                };
            }
        });
        List<MonitoringLogsDto> result = dataCache.getAndRemoveAll();
        Assertions.assertNotNull(result);
        Assertions.assertEquals(5, result.size());

        status = dataCache.getStatus();
        Assertions.assertNotNull(status);
        Assertions.assertEquals(0, status.get("counter"));

        for (int i = 0; i < 30; i++) {
            dataCache.put(new MonitoringLogsDto());
        }
        verify(cache, times(20)).put(anyString(), any());

        status = dataCache.getStatus();
        Assertions.assertNotNull(status);
        Assertions.assertEquals(30, status.get("counter"));
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
