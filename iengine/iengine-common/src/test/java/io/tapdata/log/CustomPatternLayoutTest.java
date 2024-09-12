package io.tapdata.log;


import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;
import org.apache.logging.log4j.core.layout.LevelPatternSelector;
import org.apache.logging.log4j.core.layout.PatternMatch;
import org.apache.logging.log4j.core.pattern.RegexReplacement;
import org.apache.logging.log4j.message.SimpleMessage;
import org.junit.jupiter.api.*;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class CustomPatternLayoutTest {

    Log4jLogEvent log4jLogEvent;
    String pattern = "[%-5level] %date{yyyy-MM-dd HH:mm:ss.SSS} [%t] %c{1} - %msg%n";
    String message = "at org.tapdata.tapdata.tapdata.AbstractBufferingClientHttpRequest.tapdata(AbstractBufferingClientHttpRequest.tapdata:48)";
    @BeforeEach
    void before(){
        Log4jLogEvent.Builder loggingEventBuilder = new Log4jLogEvent.Builder();
        SimpleMessage simpleMessage = new SimpleMessage(message);
        loggingEventBuilder.setMessage(simpleMessage);
        loggingEventBuilder.setLevel(Level.INFO);
        loggingEventBuilder.setLoggerName("testLoggerName");
        log4jLogEvent = loggingEventBuilder.build();
    }

    @Nested
    class NormalPatternTest {
        AbstractStringLayout.Serializer customPatternLayout;
        @BeforeEach
        void beforeAll() {
            CustomPatternLayout.SerializerBuilder serializerBuilder = new CustomPatternLayout.SerializerBuilder();
            serializerBuilder.setPattern(pattern);
            customPatternLayout = serializerBuilder.build();
        }

        @DisplayName("test normal pattern not oem Serializable")
        @Test
        void test1() {
            StringBuilder stringBuilder = new StringBuilder();
            StringBuilder convertString = customPatternLayout.toSerializable(log4jLogEvent, stringBuilder);
            Assertions.assertEquals(true, convertString.toString().contains(message));
        }

        @DisplayName("test normal pattern oem Serializable")
        @Test
        void test2() {
            Map<String, Object> oemMap = new HashMap<>();
            oemMap.put("tapdata", "oem");
            StringBuilder stringBuilder = new StringBuilder();
            ReflectionTestUtils.setField(customPatternLayout, "oemConfigMap", oemMap);
            StringBuilder convertString = customPatternLayout.toSerializable(log4jLogEvent, stringBuilder);
            Assertions.assertEquals(false, convertString.toString().contains(message));
        }

        @DisplayName("test normal pattern replace Serializable")
        @Test
        void test3() {
            RegexReplacement regexReplacement = RegexReplacement.createRegexReplacement(Pattern.compile("org"), "io");
            CustomPatternLayout.SerializerBuilder serializerBuilder = new CustomPatternLayout.SerializerBuilder();
            serializerBuilder.setPattern(pattern);
            serializerBuilder.setReplace(regexReplacement);
            AbstractStringLayout.Serializer customPatternLayout = serializerBuilder.build();
            StringBuilder stringBuilder = new StringBuilder();
            StringBuilder convertString = customPatternLayout.toSerializable(log4jLogEvent, stringBuilder);
            Assertions.assertEquals(false, convertString.toString().contains(message));
        }

    }
    @Nested
    class PatternSelectTest{
        LevelPatternSelector patternSelector;

        @BeforeEach
        void beforeAll() {
            String infoPattern="[%-5level] %date{yyyy-MM-dd HH:mm:ss.SSS} %msg%n";
            PatternMatch level = PatternMatch.newBuilder()
                    .setPattern(infoPattern)
                    .setKey("INFO").build();
            PatternMatch[] patternMatchList = new PatternMatch[1];
            patternMatchList[0] = level;
            LevelPatternSelector.Builder patternSelectBuilder = LevelPatternSelector.newBuilder();
            patternSelectBuilder.setProperties(patternMatchList);
            patternSelectBuilder.setDefaultPattern(pattern);
            patternSelector = patternSelectBuilder.build();
        }
        @DisplayName("test patternSelector not oem Serializable")
        @Test
        void test4() {
            CustomPatternLayout.SerializerBuilder serializerBuilder = new CustomPatternLayout.SerializerBuilder();
            serializerBuilder.setPatternSelector(patternSelector);
            serializerBuilder.setDefaultPattern(pattern);

            AbstractStringLayout.Serializer patternSelectLayout = serializerBuilder.build();
            StringBuilder stringBuilder = new StringBuilder();
            StringBuilder covertString = patternSelectLayout.toSerializable(log4jLogEvent, stringBuilder);

            Assertions.assertEquals(true, covertString.toString().contains(message));
        }
        @DisplayName("test patternSelector oem Serializable")
        @Test
        void test5(){
            CustomPatternLayout.SerializerBuilder serializerBuilder = new CustomPatternLayout.SerializerBuilder();
            serializerBuilder.setPatternSelector(patternSelector);
            serializerBuilder.setDefaultPattern(pattern);
            AbstractStringLayout.Serializer patternSelectLayout = serializerBuilder.build();
            Map<String, Object> oemMap = new HashMap<>();
            oemMap.put("tapdata", "oem");
            StringBuilder stringBuilder = new StringBuilder();

            ReflectionTestUtils.setField(patternSelectLayout, "oemConfigMap", oemMap);
            StringBuilder covertString = patternSelectLayout.toSerializable(log4jLogEvent, stringBuilder);
            Assertions.assertEquals(false,covertString.toString().contains(message));
        }
        @DisplayName("test patternSelector oem Serializable and have replace")
        @Test
        void test6(){
            RegexReplacement regexReplacement = RegexReplacement.createRegexReplacement(Pattern.compile("org"), "io");
            CustomPatternLayout.SerializerBuilder serializerBuilder = new CustomPatternLayout.SerializerBuilder();
            serializerBuilder.setPatternSelector(patternSelector);
            serializerBuilder.setDefaultPattern(pattern);
            serializerBuilder.setReplace(regexReplacement);
            AbstractStringLayout.Serializer patternSelectLayout = serializerBuilder.build();
            Map<String, Object> oemMap = new HashMap<>();
            oemMap.put("tapdata", "oem");
            StringBuilder stringBuilder = new StringBuilder();

            ReflectionTestUtils.setField(patternSelectLayout, "oemConfigMap", oemMap);
            StringBuilder covertString = patternSelectLayout.toSerializable(log4jLogEvent, stringBuilder);
            Assertions.assertEquals(false,covertString.toString().contains(message));
        }
    }


}
