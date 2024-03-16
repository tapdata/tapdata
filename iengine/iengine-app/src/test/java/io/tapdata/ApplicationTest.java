package io.tapdata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
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
            assertEquals(2,context.getRootLogger().getAppenders().size());
        }
    }
}