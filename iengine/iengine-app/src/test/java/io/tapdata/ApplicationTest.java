package io.tapdata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ApplicationTest {
    @Nested
    class AddRollingFileAppenderTest{
        @Test
        void test1(){
            assertDoesNotThrow(()->{Application.addRollingFileAppender("./workDir");});
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            RollingFileAppender appender =(RollingFileAppender)context.getRootLogger().getAppenders().get("rollingFileAppender");
            assertEquals("./workDir/logs/agent/tapdata-agent.log",appender.getFileName());
        }
    }
}
