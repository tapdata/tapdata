package com.tapdata.tm.utils;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class CustomPatternLayoutTest {
    LoggerContext loggerContext;
    LoggingEvent iLoggingEvent=new LoggingEvent();
    String message;
    CustomPatternLayout customPatternLayout = new CustomPatternLayout();

    @BeforeEach
    void beforeEach(){
        loggerContext= new LoggerContext();
        loggerContext.setName("managerContext");
        message = "at org.tapdata.tapdata.tapdata.AbstractBufferingClientHttpRequest.tapdata(AbstractBufferingClientHttpRequest.tapdata:48)";
        iLoggingEvent.setLevel(Level.INFO);
        iLoggingEvent.setMessage(message);
        customPatternLayout.setPattern("%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level  - %msg%n");
        customPatternLayout.setContext(loggerContext);
        customPatternLayout.start();
    }
    @Test
    void test1(){
        customPatternLayout.setContext(loggerContext);
        customPatternLayout.start();
        String formatLayout = customPatternLayout.doLayout(iLoggingEvent);
        assertEquals(true, formatLayout.contains(message));
    }
    @Test
    void test2(){
        Map<String,Object> oemConfigMap=new HashMap<>();
        oemConfigMap.put("tapdata","oem");
        customPatternLayout.setOemConfigmap(oemConfigMap);
        customPatternLayout.setContext(loggerContext);
        customPatternLayout.start();
        String formatLayout = customPatternLayout.doLayout(iLoggingEvent);
        assertEquals(false,formatLayout.contains(message));
    }


}
