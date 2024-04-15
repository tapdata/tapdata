package io.tapdata.Schedule.Watcher;

import com.tapdata.constant.BeanUtil;
import com.tapdata.mongo.HttpClientMongoOperator;
import io.tapdata.common.CustomHttpAppender;
import io.tapdata.common.SettingService;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.observable.logging.util.Conf.LogConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.HttpAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.*;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

public class AgentLogConfigurationWatcherTest {
    @Test
    void getLogConfTest(){
        AgentLogConfigurationWatcher agentLogConfigurationWatcher = new AgentLogConfigurationWatcher();
        try(MockedStatic<ObsLoggerFactory> obsLoggerFactoryMockedStatic = mockStatic(ObsLoggerFactory.class);
            MockedStatic<BeanUtil> beanUtilMockedStatic = mockStatic(BeanUtil.class)){
            ObsLoggerFactory obsLoggerFactory = mock(ObsLoggerFactory.class);
            when(ObsLoggerFactory.getInstance()).thenReturn(obsLoggerFactory);
            SettingService settingService = mock(SettingService.class);
            when(BeanUtil.getBean(SettingService.class)).thenReturn(settingService);
            when(settingService.getString("scriptEngineHttpAppender", "false")).thenReturn("false");
            when(settingService.getString("logLevel","info")).thenReturn("info");
            LogConfiguration logConfiguration = LogConfiguration.builder().logSaveTime(180).logSaveSize(10).logSaveCount(10).build();
            when(obsLoggerFactory.getLogConfiguration("agent")).thenReturn(logConfiguration);
            LogConfiguration logConf = agentLogConfigurationWatcher.getLogConfig();
            assertEquals(logConfiguration,logConf);
            assertEquals("info",logConf.getLogLevel());
            assertEquals("false",logConf.getScriptEngineHttpAppender());
            assertEquals(180,logConf.getLogSaveTime());
            assertEquals(10,logConf.getLogSaveSize());
            assertEquals(10,logConf.getLogSaveCount());
        }
    }
    @DisplayName("test Update Config null")
    @Test
    void updateConfigTest(){
        AgentLogConfigurationWatcher agentLogConfigurationWatcher=new AgentLogConfigurationWatcher();
        LogConfiguration logConfiguration = LogConfiguration.builder().logSaveTime(180).logSaveSize(10).logSaveCount(10).logLevel("info").scriptEngineHttpAppender("false").build();
        assertDoesNotThrow(()->{agentLogConfigurationWatcher.updateConfig(logConfiguration);});
    }
    @DisplayName("test  updateRollingFileAppender normal")
    @Test
    void updateRollingFileAppenderTest(){
        LoggerContext context = LoggerContext.getContext(false);
        AgentLogConfigurationWatcher agentLogConfigurationWatcher=new AgentLogConfigurationWatcher();
        LogConfiguration logConfiguration = LogConfiguration.builder().logSaveTime(180).logSaveSize(10).logSaveCount(100).build();
        Logger rootLogger = context.getRootLogger();
        TimeBasedTriggeringPolicy timeBasedTriggeringPolicy = TimeBasedTriggeringPolicy.newBuilder().withInterval(1).withModulate(true).build();
        RollingFileAppender rollingFileAppender = RollingFileAppender.newBuilder()
                .setName("rollingFileAppender")
                .withFileName("./tapdata-agent.log")
                .withFilePattern("./tapdata-agent-%i.log.%d{yyyyMMdd}.gz")
                .withPolicy(timeBasedTriggeringPolicy)
                .build();
        rootLogger.addAppender(rollingFileAppender);
        agentLogConfigurationWatcher.updateRollingFileAppender(logConfiguration);
        context.getRootLogger().getAppenders().get("rollingFileAppender");
        Appender updateAppender = rootLogger.getAppenders().get("rollingFileAppender");
        RollingFileAppender updateRollAppender = (RollingFileAppender) updateAppender;
        TriggeringPolicy triggeringPolicy = updateRollAppender.getTriggeringPolicy();
        boolean isUpdateSuccess=triggeringPolicy instanceof CompositeTriggeringPolicy;
        assertEquals(true,isUpdateSuccess);
        DefaultRolloverStrategy rolloverStrategy = (DefaultRolloverStrategy)updateRollAppender.getManager().getRolloverStrategy();
        assertEquals(100,rolloverStrategy.getMaxIndex());
    }

    @DisplayName("test Update logLevel with info and logger add root logger appender")
    @Test
    void updateLogLevelTest(){
        LoggerContext context = LoggerContext.getContext(false);
        org.apache.logging.log4j.core.Logger logger = (Logger) LogManager.getLogger("io.tapdata.CustomProcessor.123");
        Map<String, Appender> appenders = logger.get().getAppenders();
        HttpAppender mock = mock(HttpAppender.class);
        PatternLayout patternLayout = PatternLayout.newBuilder()
                .withPattern("[%-5level] %date{yyyy-MM-dd HH:mm:ss.SSS} %X{taskId} [%t] %c{1} - %msg%n")
                .build();
        logger.addAppender(CustomHttpAppender.createAppender("httpAppender",null,patternLayout,mock(HttpClientMongoOperator.class)));
        AgentLogConfigurationWatcher agentLogConfigurationWatcher=new AgentLogConfigurationWatcher();
        LogConfiguration logConfiguration = LogConfiguration.builder().logSaveTime(180).logSaveSize(10).logSaveCount(10).logLevel("info").scriptEngineHttpAppender("false").build();
        agentLogConfigurationWatcher.updateLogLevel(logConfiguration);
        assertEquals("INFO",logger.getLevel().name());
        assertEquals(1,logger.getAppenders().size());
        assertEquals(false,logger.get().isAdditive());
    }
    @DisplayName("test Update logLevel with debug")
    @Test
    void updateLogLevelTest2(){
        LoggerContext context = LoggerContext.getContext(false);
        org.apache.logging.log4j.core.Logger logger = (Logger) LogManager.getLogger("com.tapdata.CustomProcessor.123");

        AgentLogConfigurationWatcher agentLogConfigurationWatcher=new AgentLogConfigurationWatcher();
        LogConfiguration logConfiguration = LogConfiguration.builder().logSaveTime(180).logSaveSize(10).logSaveCount(10).logLevel("debug").scriptEngineHttpAppender("false").build();
        agentLogConfigurationWatcher.updateLogLevel(logConfiguration);
        assertEquals("DEBUG",logger.getLevel().name());
        assertEquals(true,logger.isAdditive());
    }
    @DisplayName("test Update logLevel with info, but logger use parent additive")
    @Test
    void updateLogLevelTest3(){
        LoggerContext context = LoggerContext.getContext(false);
        org.apache.logging.log4j.core.Logger logger = (Logger) LogManager.getLogger("com.tapdata.CustomProcessor.123");
        AgentLogConfigurationWatcher agentLogConfigurationWatcher=new AgentLogConfigurationWatcher();
        LogConfiguration logConfiguration = LogConfiguration.builder().logSaveTime(180).logSaveSize(10).logSaveCount(10).logLevel("debug").scriptEngineHttpAppender("true").build();
        agentLogConfigurationWatcher.updateLogLevel(logConfiguration);
        assertEquals("INFO",logger.getLevel().name());
    }
    @DisplayName("test check is modify agentLogConfig is null")
    @Test
    void checkIsModifyTest1(){
        AgentLogConfigurationWatcher agentLogConfigurationWatcher = new AgentLogConfigurationWatcher();
        LogConfiguration logConfiguration = LogConfiguration.builder().logSaveTime(180).logSaveSize(10).logSaveCount(10).logLevel("debug").scriptEngineHttpAppender("true").build();
        agentLogConfigurationWatcher.checkIsModify(logConfiguration);
    }
    @DisplayName("test check is modify loglevel is modify")
    @Test
    void checkIsModifyTest2(){
        AgentLogConfigurationWatcher agentLogConfigurationWatcher = new AgentLogConfigurationWatcher();
        LogConfiguration logConfiguration = LogConfiguration.builder().logSaveTime(180).logSaveSize(10).logSaveCount(10).logLevel("debug").scriptEngineHttpAppender("true").build();
        LogConfiguration logConfiguration1 = LogConfiguration.builder().logSaveTime(180).logSaveSize(10).logSaveCount(10).logLevel("debug").scriptEngineHttpAppender("false").build();
        agentLogConfigurationWatcher.logConfiguration=logConfiguration1;
        boolean result = agentLogConfigurationWatcher.checkIsModify(logConfiguration);
        assertEquals(true,result);
    }
}