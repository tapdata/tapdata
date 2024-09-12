package io.tapdata.log;


import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;
import org.apache.logging.log4j.core.pattern.RegexReplacement;
import org.apache.logging.log4j.message.SimpleMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class CustomPatternLayoutTest {
    AbstractStringLayout.Serializer customPatternLayout;
    Log4jLogEvent log4jLogEvent;
    @BeforeEach
    void beforeAll(){
        CustomPatternLayout.SerializerBuilder serializerBuilder = new CustomPatternLayout.SerializerBuilder();
        serializerBuilder.setPattern("[%-5level] %date{yyyy-MM-dd HH:mm:ss.SSS} [%t] %c{1} - %msg%n");
        customPatternLayout= serializerBuilder.build();
        String message="at org.tapdata.tapdata.tapdata.AbstractBufferingClientHttpRequest.tapdata(AbstractBufferingClientHttpRequest.tapdata:48)";
        Log4jLogEvent.Builder loggingEventBuilder = new Log4jLogEvent.Builder();
        SimpleMessage simpleMessage=new SimpleMessage(message);
        loggingEventBuilder.setMessage(simpleMessage);
        loggingEventBuilder.setLevel(Level.INFO);
        loggingEventBuilder.setLoggerName("testLoggerName");
        log4jLogEvent = loggingEventBuilder.build();
    }
    @DisplayName("test not oem Serializable")
    @Test
    void test1() {
        StringBuilder stringBuilder = new StringBuilder();
        StringBuilder convertString = customPatternLayout.toSerializable(log4jLogEvent, stringBuilder);
        Assertions.assertEquals(true,convertString.toString().contains("at org.tapdata.tapdata.tapdata.AbstractBufferingClientHttpRequest.tapdata(AbstractBufferingClientHttpRequest.tapdata:48)"));
    }
    @DisplayName("test oem Serializable")
    @Test
    void test2(){
        Map<String,Object> oemMap=new HashMap<>();
        oemMap.put("tapdata","oem");
        StringBuilder stringBuilder = new StringBuilder();
        ReflectionTestUtils.setField(customPatternLayout,"oemConfigMap",oemMap);
        StringBuilder convertString = customPatternLayout.toSerializable(log4jLogEvent, stringBuilder);
        Assertions.assertEquals(false,convertString.toString().contains("at org.tapdata.tapdata.tapdata.AbstractBufferingClientHttpRequest.tapdata(AbstractBufferingClientHttpRequest.tapdata:48)"));
    }
    @DisplayName("test replace Serializable")
    @Test
    void test3(){
        RegexReplacement regexReplacement = RegexReplacement.createRegexReplacement(Pattern.compile("org"), "io");
        CustomPatternLayout.SerializerBuilder serializerBuilder = new CustomPatternLayout.SerializerBuilder();
        serializerBuilder.setPattern("[%-5level] %date{yyyy-MM-dd HH:mm:ss.SSS} [%t] %c{1} - %msg%n");
        serializerBuilder.setReplace(regexReplacement);
        AbstractStringLayout.Serializer customPatternLayout = serializerBuilder.build();
        StringBuilder stringBuilder = new StringBuilder();
        StringBuilder convertString = customPatternLayout.toSerializable(log4jLogEvent, stringBuilder);
        Assertions.assertEquals(false,convertString.toString().contains("at org.tapdata.tapdata.tapdata.AbstractBufferingClientHttpRequest.tapdata(AbstractBufferingClientHttpRequest.tapdata:48)"));
    }
}
