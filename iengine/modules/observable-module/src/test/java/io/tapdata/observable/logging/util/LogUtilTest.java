package io.tapdata.observable.logging.util;

import io.tapdata.observable.logging.appender.AppenderFactory;
import io.tapdata.observable.logging.util.LogUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.rolling.CompositeTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.TriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.action.DeleteAction;
import org.apache.logging.log4j.core.config.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;


public class LogUtilTest {
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    @DisplayName("test AppenderFactory get getDeleteAction normal")
    @Test
    void getDeleteActionTest1(){

        Configuration configuration = context.getConfiguration();
        String glob = "tapdata-agent-*.log.*.gz";
        DeleteAction deleteAction = LogUtil.getDeleteAction(3, "./workDir/agent", glob, configuration);
        Assertions.assertEquals("./workDir/agent", deleteAction.getBasePath().toString());
        Assertions.assertEquals(2,deleteAction.getMaxDepth());
    }
    @DisplayName("test AppenderFactory getDeleteAction throw exception")
    @Test
    void getDeleteActionTest2(){
        Configuration configuration = context.getConfiguration();
        String glob = "tapdata-agent-*.log.*.gz";
        IllegalArgumentException illegalArgumentException = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            LogUtil.getDeleteAction(-3, "./workDir/agent", glob, configuration);
        });
        Assertions.assertEquals("Text cannot be parsed to a Duration: -3d",illegalArgumentException.getMessage());
    }
    @DisplayName("test AppenderFactory getCompositeTriggeringPolicyTest normal")
    @Test
    void getCompositeTriggeringPolicyTest1(){
        CompositeTriggeringPolicy compositeTriggeringPolicy = LogUtil.getCompositeTriggeringPolicy("10");
        TriggeringPolicy[] triggeringPolicies = compositeTriggeringPolicy.getTriggeringPolicies();
        Assertions.assertEquals(2,triggeringPolicies.length);
    }
    @DisplayName("test AppenderFactory getCompositeTriggeringPolicyTest by vaild log file size, retrun defualt value")
    @Test
    void getCompositeTriggeringPolicyTest2(){
        CompositeTriggeringPolicy compositeTriggeringPolicy = LogUtil.getCompositeTriggeringPolicy("-10");
        TriggeringPolicy[] triggeringPolicies = compositeTriggeringPolicy.getTriggeringPolicies();
        Assertions.assertEquals(2,triggeringPolicies.length);
    }
    @DisplayName("test get Loglevel with null levelName")
    @Test
    void logLevelTest1(){
        Level level = LogUtil.logLevel(null);
        Assertions.assertEquals(level.INFO.name(),level.name());
    }
    @DisplayName("test get Loglevel with normal Value")
    @Test
    void logLevelTest2(){
        Level level = LogUtil.logLevel("warn");
        Assertions.assertEquals(Level.WARN.name(),level.name());
    }
    @DisplayName("test get Loglevel with other value")
    @Test
    void logLevelTest3(){
        Level level = LogUtil.logLevel("abc");
        Assertions.assertEquals(Level.INFO.name(),level.name());
    }
    @DisplayName("test get Loglevel with info value")
    @Test
    void logLevelTest4(){
        Level level = LogUtil.logLevel("info");
        Assertions.assertEquals(Level.INFO.name(),level.name());
    }
    @DisplayName("test get Loglevel with debug value")
    @Test
    void logLevelTest5(){
        Level level = LogUtil.logLevel("debug");
        Assertions.assertEquals(Level.DEBUG.name(),level.name());
    }
    @DisplayName("test get Loglevel with debug value")
    @Test
    void logLevelTest6(){
        Level level = LogUtil.logLevel("trace");
        Assertions.assertEquals(Level.TRACE.name(),level.name());
    }
    @DisplayName("test get Loglevel with debug value")
    @Test
    void logLevelTest7(){
        Level level = LogUtil.logLevel("error");
        Assertions.assertEquals(Level.ERROR.name(),level.name());
    }
}