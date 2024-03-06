package io.tapdata.observable.logging.appender;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.rolling.CompositeTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.TriggeringPolicy;
import org.apache.logging.log4j.core.appender.rolling.action.DeleteAction;
import org.apache.logging.log4j.core.config.Configuration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class AppenderFactoryTest {
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    @DisplayName("test AppenderFactory get getDeleteAction normal")
    @Test
    void getDeleteActionTest1(){

        Configuration configuration = context.getConfiguration();
        String glob = "tapdata-agent-*.log.*.gz";
        DeleteAction deleteAction = AppenderFactory.getInstance().getDeleteAction(3, "./workDir/agent", glob, configuration);
        assertEquals("./workDir/agent", deleteAction.getBasePath().toString());
        assertEquals(2,deleteAction.getMaxDepth());
    }
    @DisplayName("test AppenderFactory getDeleteAction throw exception")
    @Test
    void getDeleteActionTest2(){
        Configuration configuration = context.getConfiguration();
        String glob = "tapdata-agent-*.log.*.gz";
        IllegalArgumentException illegalArgumentException = assertThrows(IllegalArgumentException.class, () -> {
            AppenderFactory.getInstance().getDeleteAction(-3, "./workDir/agent", glob, configuration);
        });
        assertEquals("Text cannot be parsed to a Duration: -3d",illegalArgumentException.getMessage());
    }
    @DisplayName("test AppenderFactory getCompositeTriggeringPolicyTest normal")
    @Test
    void getCompositeTriggeringPolicyTest1(){
        CompositeTriggeringPolicy compositeTriggeringPolicy = AppenderFactory.getInstance().getCompositeTriggeringPolicy("10");
        TriggeringPolicy[] triggeringPolicies = compositeTriggeringPolicy.getTriggeringPolicies();
        assertEquals(2,triggeringPolicies.length);
    }
    @DisplayName("test AppenderFactory getCompositeTriggeringPolicyTest by vaild log file size, retrun defualt value")
    @Test
    void getCompositeTriggeringPolicyTest2(){
        CompositeTriggeringPolicy compositeTriggeringPolicy = AppenderFactory.getInstance().getCompositeTriggeringPolicy("-10");
        TriggeringPolicy[] triggeringPolicies = compositeTriggeringPolicy.getTriggeringPolicies();
        assertEquals(2,triggeringPolicies.length);
    }

}
