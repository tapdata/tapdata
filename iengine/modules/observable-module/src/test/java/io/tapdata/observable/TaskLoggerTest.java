package io.tapdata.observable;

import io.tapdata.observable.logging.TaskLogger;
import io.tapdata.observable.logging.appender.AppenderFactory;
import io.tapdata.observable.logging.appender.FileAppender;
import io.tapdata.observable.logging.appender.ObsHttpTMAppender;
import io.tapdata.observable.logging.util.Conf.LogConfiguration;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.CompositeTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.TimeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.TriggeringPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class TaskLoggerTest {
    @DisplayName("test Refresh FileAppender")
    @Test
    void test1(){
        try(MockedStatic<AppenderFactory> appenderFactoryMockedStatic = mockStatic(AppenderFactory.class)){
            List<io.tapdata.observable.logging.appender.Appender<?>> tapObsAppenders = new ArrayList<>();
            LogConfiguration logConfiguration = new LogConfiguration(180, 10, 100);
            AppenderFactory appenderFactory = mock(AppenderFactory.class);
            when(AppenderFactory.getInstance()).thenReturn(appenderFactory);
            doCallRealMethod().when(appenderFactory).getCompositeTriggeringPolicy(logConfiguration.getLogSaveSize().toString());
            TaskLogger taskLogger = mock(TaskLogger.class);
            TimeBasedTriggeringPolicy timeBasedTriggeringPolicy = TimeBasedTriggeringPolicy.newBuilder().withInterval(1).withModulate(true).build();
            RollingFileAppender rollingFileAppender = RollingFileAppender.newBuilder()
                    .setName("rollingFileAppender")
                    .withFileName("./tapdata-agent.log")
                    .withFilePattern("./tapdata-agent-%i.log.%d{yyyyMMdd}.gz")
                    .withPolicy(timeBasedTriggeringPolicy)
                    .build();
            FileAppender fileAppender = mock(FileAppender.class);
            ReflectionTestUtils.setField(fileAppender,"rollingFileAppender",rollingFileAppender);
            doCallRealMethod().when(fileAppender).getRollingFileAppender();
            tapObsAppenders.add(fileAppender);
            ReflectionTestUtils.setField(taskLogger,"tapObsAppenders",tapObsAppenders);
            ReflectionTestUtils.setField(taskLogger,"logAppendFactory",appenderFactory);
            doCallRealMethod().when(taskLogger).refreshFileAppender(logConfiguration);
            taskLogger.refreshFileAppender(logConfiguration);
            assertEquals("./tapdata-agent.log",rollingFileAppender.getFileName());
            assertEquals("rollingFileAppender",rollingFileAppender.getName());
            TriggeringPolicy triggeringPolicy = rollingFileAppender.getTriggeringPolicy();
            boolean updateSuccess= triggeringPolicy instanceof CompositeTriggeringPolicy;
            assertEquals(true,updateSuccess);
        }

    }

    @DisplayName("test appenders list no have FileAppender ")
    @Test
    void test2() {
        LogConfiguration logConfiguration = new LogConfiguration(180, 10, 100);
        TaskLogger taskLogger = mock(TaskLogger.class);
        List<io.tapdata.observable.logging.appender.Appender<?>> tapObsAppenders = new ArrayList<>();
        ObsHttpTMAppender obsHttpTMAppender = mock(ObsHttpTMAppender.class);
        tapObsAppenders.add(obsHttpTMAppender);
        ReflectionTestUtils.setField(taskLogger, "tapObsAppenders", tapObsAppenders);
        doCallRealMethod().when(taskLogger).refreshFileAppender(logConfiguration);
        assertEquals(1, tapObsAppenders.size());
        assertEquals(true, obsHttpTMAppender == tapObsAppenders.get(0));
    }
}
