package io.tapdata.observable.logging.appender;

import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.observable.logging.util.Conf.LogConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class FileAppenderTest {
    @Test
    public void test1(){
        try (MockedStatic<AppenderFactory> appenderFactoryMockedStatic = mockStatic(AppenderFactory.class)) {
            LogConfiguration logConfiguration = new LogConfiguration(180, 10, 100);
            AppenderFactory appenderFactory = mock(AppenderFactory.class);
            when(AppenderFactory.getInstance()).thenReturn(appenderFactory);
            doCallRealMethod().when(appenderFactory).getCompositeTriggeringPolicy(logConfiguration.getLogSaveSize().toString());
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
}
