package io.tapdata.observable.logging.appender;

import com.tapdata.constant.BeanUtil;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import io.tapdata.observable.logging.LogLevel;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.observable.logging.debug.DataCache;
import io.tapdata.observable.logging.debug.DataCacheFactory;
import io.tapdata.observable.logging.util.Conf.LogConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.ehcache.Cache;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class FileAppenderTest {
    @Test
    public void test1(){
        try (MockedStatic<AppenderFactory> appenderFactoryMockedStatic = mockStatic(AppenderFactory.class)) {
            LogConfiguration logConfiguration = LogConfiguration.builder().logSaveTime(180).logSaveSize(10).logSaveCount(100).build();
            AppenderFactory appenderFactory = mock(AppenderFactory.class);
            when(AppenderFactory.getInstance()).thenReturn(appenderFactory);
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            Logger test = LogManager.getLogger("test");
            when(obsLoggerFactory.getLogConfiguration("task")).thenReturn(logConfiguration);
            FileAppender fileAppender = mock(FileAppender.class);
            ReflectionTestUtils.setField(fileAppender, "logger", test);
            ReflectionTestUtils.setField(fileAppender, "obsLoggerFactory", obsLoggerFactory);
            ReflectionTestUtils.setField(fileAppender, "taskId", "test123");
            doCallRealMethod().when(fileAppender).start();
            fileAppender.start();
            org.apache.logging.log4j.core.Logger test1 = (org.apache.logging.log4j.core.Logger) test;
            Map<String, Appender> appenders = test1.getAppenders();
            Appender appender = appenders.get("rollingFileAppender-test123");
            RollingFileAppender rollingFileAppender = null;
            if (appender instanceof RollingFileAppender) {
                rollingFileAppender = (RollingFileAppender) appender;
            }
            assertEquals("rollingFileAppender-test123", rollingFileAppender.getName());
            assertEquals("logs/jobs/test123.log", rollingFileAppender.getFileName());
        }
    }

    @Nested
    class testIncludeLevel {

        private FileAppender fileAppender;
        private Logger logger;

        @BeforeEach
        void before() {
            try (MockedStatic<BeanUtil> mockStaticBeanUtil = mockStatic(BeanUtil.class)) {
                mockStaticBeanUtil.when(() -> BeanUtil.getBean(any())).thenAnswer(answer -> {
                    Class<?> cls = answer.getArgument(0);
                    return mock(cls);
                });
                fileAppender = FileAppender.create("", "taskId");
                Assertions.assertNotNull(fileAppender);
            }

            logger = mock(Logger.class);
            ReflectionTestUtils.setField(fileAppender, "logger", logger);
        }

        @org.junit.jupiter.api.Test
        void testOnlyDebug() {
            fileAppender.include().include(LogLevel.DEBUG);

            Object includeLevel = ReflectionTestUtils.getField(fileAppender, "includeLogLevel");
            Assertions.assertNotNull(includeLevel);
            Assertions.assertEquals(1, ((Set)includeLevel).size());

            fileAppender.append(MonitoringLogsDto.builder().level(LogLevel.DEBUG.getLevel()).build());
            fileAppender.append(MonitoringLogsDto.builder().level(LogLevel.DEBUG.getLevel()).build());

            fileAppender.openCatchData();

            fileAppender.append(MonitoringLogsDto.builder().level(LogLevel.DEBUG.getLevel()).build());
            fileAppender.append(MonitoringLogsDto.builder().level(LogLevel.INFO.getLevel()).build());
            fileAppender.append(MonitoringLogsDto.builder().logTag("catchData").level(LogLevel.DEBUG.getLevel()).build());
            fileAppender.append(MonitoringLogsDto.builder().logTag("catchData").level(LogLevel.DEBUG.getLevel()).build());
            fileAppender.append(MonitoringLogsDto.builder().level(LogLevel.TRACE.getLevel()).build());

            verify(logger, times(2)).debug(anyString());
            verify(logger, times(0)).info(anyString());
            verify(logger, times(0)).trace(anyString());
        }

        @org.junit.jupiter.api.Test
        void testInclude() {
            fileAppender.include().include(LogLevel.DEBUG).include(LogLevel.DEBUG).include(LogLevel.INFO);
            Object includeLevel = ReflectionTestUtils.getField(fileAppender, "includeLogLevel");
            Assertions.assertNotNull(includeLevel);
            Assertions.assertEquals(2, ((Set)includeLevel).size());
        }

        @org.junit.jupiter.api.Test
        void testTrace() {
            fileAppender.openCatchData();
            fileAppender.append(MonitoringLogsDto.builder().level(LogLevel.TRACE.getLevel()).build());
            fileAppender.append(MonitoringLogsDto.builder().logTag("catchData").level(LogLevel.DEBUG.getLevel()).build());
            fileAppender.append(MonitoringLogsDto.builder().level(LogLevel.INFO.getLevel()).build());
            fileAppender.append(MonitoringLogsDto.builder().level(LogLevel.WARN.getLevel()).build());
            fileAppender.append(MonitoringLogsDto.builder().level(LogLevel.ERROR.getLevel()).build());
            fileAppender.append(MonitoringLogsDto.builder().level(LogLevel.FATAL.getLevel()).build());


            verify(logger, times(1)).debug(anyString());
            verify(logger, times(1)).info(anyString());
            verify(logger, times(1)).trace(anyString());
        }

        @org.junit.jupiter.api.Test
        void testCatchData() {

            DataCacheFactory dataCacheFactory = mock(DataCacheFactory.class);
            Cache cache = mock(Cache.class);
            DataCache dataCache = new DataCache("taskId", 100L, cache);
            when(dataCacheFactory.getDataCache("taskId")).thenReturn(dataCache);

            try (MockedStatic<DataCacheFactory> mockDataCacheFactory = mockStatic(DataCacheFactory.class)) {

                mockDataCacheFactory.when(DataCacheFactory::getInstance).thenReturn(dataCacheFactory);

                boolean result = fileAppender.openCatchData();
                Assertions.assertTrue(result);

                fileAppender.append(MonitoringLogsDto.builder().level(LogLevel.DEBUG.getLevel()).build());
                fileAppender.taskId = "taskId_debug";
                fileAppender.append(MonitoringLogsDto.builder().logTag("catchData").logTag("eid=e1").level(LogLevel.DEBUG.getLevel()).build());
                fileAppender.taskId = "taskId";
                fileAppender.append(MonitoringLogsDto.builder().logTag("catchData").logTag("eid=e2").level(LogLevel.DEBUG.getLevel()).build());
                verify(cache, times(2)).put(any(), any());

                result = fileAppender.closeCatchData();
                Assertions.assertTrue(result);

                fileAppender.append(MonitoringLogsDto.builder().logTag("catchData").level(LogLevel.DEBUG.getLevel()).build());
                verify(cache, times(2)).put(any(), any());

                ReflectionTestUtils.setField(fileAppender, "taskId", null);
                Assertions.assertDoesNotThrow(() -> {
                    fileAppender.closeCatchData();
                });

            }

        }
    }
}