package io.tapdata.log;


import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;
import org.apache.logging.log4j.message.SimpleMessage;
import org.junit.jupiter.api.Test;

public class CustomPatternLayoutTest {
    @Test
    void test1() {
        CustomPatternLayout.SerializerBuilder serializerBuilder = new CustomPatternLayout.SerializerBuilder();
        serializerBuilder.setPattern("[%-5level] %date{yyyy-MM-dd HH:mm:ss.SSS} [%t] %c{1} - %msg%n");
        AbstractStringLayout.Serializer pattern = serializerBuilder.build();
        String message="at org.tapdata.tapdata.tapdata.AbstractBufferingClientHttpRequest.tapdata(AbstractBufferingClientHttpRequest.tapdata:48)";
        Log4jLogEvent.Builder loggingEventBuilder = new Log4jLogEvent.Builder();
        SimpleMessage simpleMessage=new SimpleMessage(message);
        loggingEventBuilder.setMessage(simpleMessage);
        loggingEventBuilder.setLevel(Level.INFO);
        loggingEventBuilder.setLoggerName("testLoggerName");
        Log4jLogEvent log4jLogEvent = loggingEventBuilder.build();
        StringBuilder stringBuilder = new StringBuilder();
        StringBuilder serializable = pattern.toSerializable(log4jLogEvent, stringBuilder);
        System.out.println(serializable.toString());
    }
}
