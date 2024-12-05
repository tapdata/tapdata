package io.tapdata.observable.logging.debug;

import com.alibaba.fastjson.JSON;
import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.observable.logging.ObsLoggerFactory;
import org.bson.types.ObjectId;
import org.ehcache.Cache;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
        Map<String, DataCache.CacheItem> cacheData = new HashMap<>();
        doAnswer(answer -> {
            DataCache.CacheItem item = answer.getArgument(1);
            cacheData.put(answer.getArgument(0), item);
            return null;
        }).when(cache).put(anyString(), any());
        doAnswer(answer -> {
            String k = answer.getArgument(0);
            return cacheData.containsKey(k);
        }).when(cache).containsKey(anyString());
        doAnswer(answer -> {
            String k = answer.getArgument(0);
            return cacheData.get(k);
        }).when(cache).get(anyString());
        doAnswer(answer -> {
            Consumer consumer = answer.getArgument(0);
            cacheData.forEach((k,v) -> {
                consumer.accept(new Cache.Entry<String, DataCache.CacheItem>(){

                    @Override
                    public String getKey() {
                        return k;
                    }

                    @Override
                    public DataCache.CacheItem getValue() {
                        return v;
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
        dataCache.setMaxTotalEntries(12L);
        dataCache.put(MonitoringLogsDto.builder().logTag("catchData").logTag("eid=e0").nodeId("node-1").build());
        dataCache.put(MonitoringLogsDto.builder().logTag("catchData").logTag("eid=e0").nodeId("node-2").build());
        verify(cache, times(2)).replace(anyString(), any());
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

    @Test
    public void testDataCache() {

        AtomicInteger counter = new AtomicInteger(0);
        Cache<String, DataCache.CacheItem> cache = mock(Cache.class);
        when(cache.iterator()).thenReturn(new Iterator<Cache.Entry<String, DataCache.CacheItem>>() {
            @Override
            public boolean hasNext() {
                return counter.get() > 0;
            }

            @Override
            public Cache.Entry<String, DataCache.CacheItem> next() {
                int idx = counter.getAndDecrement();
                return new Cache.Entry<String, DataCache.CacheItem>() {
                    @Override
                    public String getKey() {
                        return "k-" + idx;
                    }

                    @Override
                    public DataCache.CacheItem getValue() {
                        Map<String, MonitoringLogsDto> logs = new HashMap<>();
                        Map<String, Object> data = new HashMap<>();
                        if (idx %2 == 0)
                            data.put("name", "test-" + idx);
                        else
                            data.put("name", "message-" + idx);
                        logs.put("node-1", MonitoringLogsDto.builder().data(Collections.singleton(data)).build());
                        return DataCache.CacheItem.builder().data(logs).build();
                    }
                };
            }

            @Override
            public void remove() {

            }
        });

        DataCache dataCache = new DataCache("taskId", null, cache);
        Assertions.assertDoesNotThrow(() -> {
            Method getEventType = dataCache.getClass().getDeclaredMethod("getEventType", List.class);
            getEventType.setAccessible(true);
            Object result = getEventType.invoke(dataCache, Collections.emptyList());
            Assertions.assertNull(result);
        });

        try (MockedStatic<ObsLoggerFactory> mockObsLoggerFactory = mockStatic(ObsLoggerFactory.class)) {
            mockObsLoggerFactory.when(ObsLoggerFactory::getInstance).thenReturn(mock(ObsLoggerFactory.class));
            Map<String, Object> result = dataCache.searchAndRemove(null, null);
            Assertions.assertNotNull(result);

            counter.set(25);
            for (int i = 0; i < 3; i++) {
                result = dataCache.searchAndRemove(null, null);
                Assertions.assertNotNull(result);
                Assertions.assertNotNull(result.get("data"));
                Assertions.assertInstanceOf(List.class, result.get("data"));

                if (i < 2) {
                    Assertions.assertEquals(10, ((List)result.get("data")).size());
                    Assertions.assertTrue((Boolean) result.get("hasMore"));
                } else {
                    Assertions.assertEquals(5, ((List)result.get("data")).size());
                    Assertions.assertFalse((Boolean) result.get("hasMore"));
                }
            }

            counter.set(20);
            result = dataCache.searchAndRemove(10, "test");
            Assertions.assertNotNull(result);
            Assertions.assertNotNull(result.get("data"));
            Assertions.assertInstanceOf(List.class, result.get("data"));
            Assertions.assertEquals(10, ((List)result.get("data")).size());
            Assertions.assertTrue((Boolean) result.get("hasMore"));

            dataCache.destroy();
            Assertions.assertEquals(0L, dataCache.getCacheSize());
            Map<String, Object> status = dataCache.getStatus();
            Assertions.assertNotNull(status);
            Assertions.assertEquals("Cache is null", status.get("cache"));
        }
    }

    @Test
    void testMarkCatchEventWhenMatched() {

        TapdataEvent event = new TapdataEvent();
        DataCache.markCatchEventWhenMatched(event, "taskId", null);
        Assertions.assertTrue(event.isCatchMe());

        DataCache.markCatchEventWhenMatched(event, "taskId", null);
        Assertions.assertTrue(event.isCatchMe());

        DataCacheFactory dataCacheFactory = mock(DataCacheFactory.class);
        DataCache dataCache = new DataCache("taskId", null, mock(Cache.class));
        when(dataCacheFactory.getDataCache("taskId")).thenReturn(dataCache);
        try (MockedStatic<DataCacheFactory> mockBeanUtil = mockStatic(DataCacheFactory.class)) {
            mockBeanUtil.when(() -> DataCacheFactory.getInstance()).thenReturn(dataCacheFactory);

            TapInsertRecordEvent insert = new TapInsertRecordEvent();
            insert.setAfter(new HashMap<>());
            insert.getAfter().put("name", "test-1");
            event.setTapEvent(insert);

            DataCache.markCatchEventWhenMatched(event, "taskId", "message");
            Assertions.assertFalse(event.isCatchMe());
            DataCache.markCatchEventWhenMatched(event, "taskId", "test");
            Assertions.assertTrue(event.isCatchMe());

            TapUpdateRecordEvent update = new TapUpdateRecordEvent();
            update.setAfter(new HashMap<>());
            update.setBefore(new HashMap<>());
            update.getBefore().put("name", "test-1");
            update.getAfter().put("name", "message-1");

            event.setTapEvent(update);
            DataCache.markCatchEventWhenMatched(event, "taskId", "test");
            Assertions.assertTrue(event.isCatchMe());

            update.getBefore().put("name", "message-1");
            event.setTapEvent(update);
            DataCache.markCatchEventWhenMatched(event, "taskId", "test");
            Assertions.assertFalse(event.isCatchMe());

            TapDeleteRecordEvent delete = new TapDeleteRecordEvent();
            delete.setBefore(new HashMap<>());
            delete.getBefore().put("name", "test-1");

            event.setTapEvent(delete);
            DataCache.markCatchEventWhenMatched(event, "taskId", "test");
            Assertions.assertTrue(event.isCatchMe());

        }
    }

    @Test
    void testDataSerialize() {
        Map<String, Object> data = new HashMap<>();
        data.put("_id", new ObjectId("674fcd885093c75db7cfc11f"));
        data.put("created", new Timestamp(1733303759151L));
        data.put("lastModify", new DateTime(new Date(1733303826479L)));
        Map<String, Object> subDoc = new HashMap<>();
        subDoc.put("_id", new ObjectId("674fcd885093c75db7cfc11d"));
        subDoc.put("created", new Timestamp(1733303759152L));
        subDoc.put("lastModify", new DateTime(new Date(1733303826480L)));
        data.put("user", new TapMapValue(subDoc));
        TapValue tapValue = new TapStringValue();
        tapValue.setOriginValue(new ObjectId());
        data.put("id", tapValue);

        tapValue = new TapStringValue();
        tapValue.setOriginValue(new ObjectId());
        tapValue.setValue("test");
        data.put("id1", tapValue);
        data.put("nullable", null);

        tapValue = new TapStringValue();
        data.put("tapValue", tapValue);

        String string = JSON.toJSON(data, DataCacheFactory.dataSerializeConfig).toString();
        System.out.println(string);
        Assertions.assertTrue(string.contains("674fcd885093c75db7cfc11f"));
        Assertions.assertTrue(string.contains("674fcd885093c75db7cfc11d"));
        Assertions.assertTrue(string.contains("1733303826479"));
        Assertions.assertTrue(string.contains("1733303826480"));
        Assertions.assertTrue(!string.contains("nullable"));

    }

    @Test
    void testDataCacheGet() {
        try (MockedStatic<ObsLoggerFactory> mockObsLoggerFactory = mockStatic(ObsLoggerFactory.class)) {
            mockObsLoggerFactory.when(ObsLoggerFactory::getInstance).thenReturn(mock(ObsLoggerFactory.class));
            Cache<String, DataCache.CacheItem> cache = mock(Cache.class);
            Map<String, DataCache.CacheItem> cacheData = new LinkedHashMap<>();
            doAnswer(answer -> {
                DataCache.CacheItem item = answer.getArgument(1);
                cacheData.put(answer.getArgument(0), item);
                return null;
            }).when(cache).put(anyString(), any());
            doAnswer(answer -> {
                String k = answer.getArgument(0);
                return cacheData.containsKey(k);
            }).when(cache).containsKey(anyString());
            doAnswer(answer -> {
                String k = answer.getArgument(0);
                return cacheData.get(k);
            }).when(cache).get(anyString());
            doAnswer(answer -> {
                Consumer consumer = answer.getArgument(0);
                cacheData.forEach((k,v) -> {
                    consumer.accept(new Cache.Entry<String, DataCache.CacheItem>(){

                        @Override
                        public String getKey() {
                            return k;
                        }

                        @Override
                        public DataCache.CacheItem getValue() {
                            return v;
                        }
                    });
                });
                return null;
            }).when(cache).forEach(any());
            AtomicReference<Iterator<Map.Entry<String, DataCache.CacheItem>>> iterator = new AtomicReference<>();
            when(cache.iterator()).thenReturn(new Iterator<Cache.Entry<String, DataCache.CacheItem>>() {
                @Override
                public boolean hasNext() {
                    if (iterator.get() == null) {
                        iterator.set( cacheData.entrySet().iterator());
                    }
                    return iterator.get().hasNext();
                }

                @Override
                public Cache.Entry<String, DataCache.CacheItem> next() {
                    Map.Entry<String, DataCache.CacheItem> item = iterator.get().next();
                    return new Cache.Entry<String, DataCache.CacheItem>() {
                        @Override
                        public String getKey() {
                            return item.getKey();
                        }

                        @Override
                        public DataCache.CacheItem getValue() {
                            return item.getValue();
                        }
                    };
                }

                @Override
                public void remove() {
                    iterator.get().remove();
                }
            });
            DataCache dataCache = new DataCache("taskId", null, cache);
            dataCache.put(MonitoringLogsDto.builder().logTag("catchData").logTag("eid=e0").nodeId("node-1").build());
            dataCache.put(MonitoringLogsDto.builder().logTag("catchData").logTag("eid=e0").nodeId("node-2").build());
            dataCache.put(MonitoringLogsDto.builder().logTag("catchData").logTag("eid=e0").nodeId("node-3").build());
            dataCache.put(MonitoringLogsDto.builder().logTag("catchData").logTag("eid=e1").nodeId("node-1").build());
            dataCache.put(MonitoringLogsDto.builder().logTag("catchData").logTag("eid=e1").nodeId("node-2").build());

            Map<String, Object> result = dataCache.searchAndRemove(10, null);

            Assertions.assertNotNull(result);
            Assertions.assertNotNull(result.get("data"));
            Assertions.assertTrue(((List)result.get("data")).isEmpty());

            dataCache.processCompleted("eid=e0");
            iterator.set(null);
            result = dataCache.searchAndRemove(10, null);
            Assertions.assertNotNull(result);
            Assertions.assertNotNull(result.get("data"));
            Assertions.assertEquals(1, ((List)result.get("data")).size());
        }
    }

}
