package io.tapdata.Schedule.Watcher;

import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.observable.logging.appender.AppenderFactory;
import io.tapdata.observable.logging.util.Conf.LogConfiguration;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.CompositeTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.TimeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.TriggeringPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

public class AgentLogConfigurationWatcherTest {
    @Test
    void getLogConfTest(){
        AgentLogConfigurationWatcher agentLogConfigurationWatcher = new AgentLogConfigurationWatcher();
        try(MockedStatic<ObsLoggerFactory> obsLoggerFactoryMockedStatic = mockStatic(ObsLoggerFactory.class)){
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            when(ObsLoggerFactory.getInstance()).thenReturn(obsLoggerFactory);
            LogConfiguration logConfiguration = new LogConfiguration(180, 100, 10);
            when(obsLoggerFactory.getLogConfiguration("agent")).thenReturn(logConfiguration);
            LogConfiguration logConf = agentLogConfigurationWatcher.getLogConfig();
            assertEquals(logConfiguration,logConf);
        }
    }
    @DisplayName("test Update Config null")
    @Test
    void updateConfigTest(){
        AgentLogConfigurationWatcher agentLogConfigurationWatcher=new AgentLogConfigurationWatcher();
        LogConfiguration logConfiguration = new LogConfiguration(180, 100, 10);
        agentLogConfigurationWatcher.updateConfig(logConfiguration);
    }
    @DisplayName("test Update Config normal")
    @Test
    void updateConfigTest2(){
        try(MockedStatic<AppenderFactory> appenderFactoryMockedStatic = mockStatic(AppenderFactory.class)){
            AppenderFactory appenderFactory = mock(AppenderFactory.class);
            when(AppenderFactory.getInstance()).thenReturn(appenderFactory);
            AgentLogConfigurationWatcher agentLogConfigurationWatcher=new AgentLogConfigurationWatcher();
            LogConfiguration logConfiguration = new LogConfiguration(180, 100, 10);
            doCallRealMethod().when(appenderFactory).getCompositeTriggeringPolicy(logConfiguration.getLogSaveSize().toString());
            Logger rootLogger = agentLogConfigurationWatcher.context.getRootLogger();
            TimeBasedTriggeringPolicy timeBasedTriggeringPolicy = TimeBasedTriggeringPolicy.newBuilder().withInterval(1).withModulate(true).build();
            RollingFileAppender rollingFileAppender = RollingFileAppender.newBuilder()
                    .setName("rollingFileAppender")
                    .withFileName("./tapdata-agent.log")
                    .withFilePattern("./tapdata-agent-%i.log.%d{yyyyMMdd}.gz")
                    .withPolicy(timeBasedTriggeringPolicy)
                    .build();
            rootLogger.addAppender(rollingFileAppender);
            agentLogConfigurationWatcher.updateConfig(logConfiguration);
            Appender updateAppender = rootLogger.getAppenders().get("rollingFileAppender");
            RollingFileAppender updateRollAppender = (RollingFileAppender) updateAppender;
            TriggeringPolicy triggeringPolicy = updateRollAppender.getTriggeringPolicy();
            boolean isUpdateSuccess=triggeringPolicy instanceof CompositeTriggeringPolicy;
            assertEquals(true,isUpdateSuccess);
            assertEquals("./tapdata-agent.log",updateRollAppender.getFileName());
        }
    }
}
