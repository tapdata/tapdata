package io.tapdata.services;

import com.tapdata.constant.BeanUtil;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import io.tapdata.common.SettingService;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.observable.logging.debug.DataCache;
import io.tapdata.observable.logging.debug.DataCacheFactory;
import org.ehcache.Cache;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/11/25 14:59
 */
public class CatchDataServiceTest {

    @Test
    void testCatchDataService() {

        CatchDataService catchDataService = new CatchDataService();

        try (MockedStatic<ObsLoggerFactory> mockObsLoggerFactory = mockStatic(ObsLoggerFactory.class);
             MockedStatic<DataCacheFactory> mockDataCacheFactory = mockStatic(DataCacheFactory.class);
        ) {

            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            DataCacheFactory dataCacheFactory = mock(DataCacheFactory.class);
            mockObsLoggerFactory.when(ObsLoggerFactory::getInstance).thenReturn(obsLoggerFactory);
            mockDataCacheFactory.when(DataCacheFactory::getInstance).thenReturn(dataCacheFactory);

            when(obsLoggerFactory.openCatchData(eq("testId"), eq(1L), eq(1L))).thenReturn(true);
            boolean result = catchDataService.openCatchData("testId", 1L, 1L, null);
            Assertions.assertTrue(result);

            result = catchDataService.openCatchData("taskId", 1L, 1L, null);
            Assertions.assertFalse(result);

            Map<String, Object> data = catchDataService.getCatchData("testId", null, null);
            Assertions.assertNotNull(data);
            Assertions.assertTrue(data.isEmpty());

            Cache<String, DataCache.CacheItem> cache = mock(Cache.class);
            when(dataCacheFactory.getDataCache(eq("taskId"))).thenReturn(
                    new DataCache("taskId", 1L, cache));

            when(cache.iterator()).thenReturn(new Iterator<Cache.Entry<String, DataCache.CacheItem>>() {
                private int counter = 0;
                @Override
                public boolean hasNext() {
                    return counter++ < 10;
                }

                @Override
                public Cache.Entry<String, DataCache.CacheItem> next() {
                    return new Cache.Entry<String, DataCache.CacheItem>() {
                        @Override
                        public String getKey() {
                            return "counter" + counter;
                        }

                        @Override
                        public DataCache.CacheItem getValue() {
                            return DataCache.CacheItem.builder().build();
                        }
                    };
                }
            });

            data = catchDataService.getCatchData("taskId", null, null);
            Assertions.assertNotNull(data);
            Assertions.assertFalse(data.isEmpty());

            when(obsLoggerFactory.closeCatchData(eq("testId"))).thenReturn(true);
            result = catchDataService.closeCatchData("testId");
            Assertions.assertTrue(result);

            Map<String, Object> status = catchDataService.getCatchDataStatus("testId");
            Assertions.assertNotNull(status);
        }
    }
}