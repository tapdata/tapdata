package com.tapdata.tm.config.micrometer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import io.firedome.MultiTaggedCounter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * @Author knight
 * @Date 2025/8/29 12:13
 */

class MicrometerLogAppenderTest {
    MultiTaggedCounter multiTaggedCounter;
    MicrometerLogAppender appender;
    ILoggingEvent iLoggingEvent;

    @BeforeEach
    void init() {
        multiTaggedCounter = mock(MultiTaggedCounter.class);
        appender = mock(MicrometerLogAppender.class);
        iLoggingEvent = mock(ILoggingEvent.class);

        ReflectionTestUtils.setField(appender, "multiTaggedCounter", multiTaggedCounter);
    }

    @Nested
    class AppendErrorLevelTest {
        @BeforeEach
        void init() {
            when(iLoggingEvent.getLevel()).thenReturn(Level.ERROR);
            doCallRealMethod().when(appender).append(iLoggingEvent);
        }

        @Test
        void testNormal() {
            appender.append(iLoggingEvent);
            verify(multiTaggedCounter, times(1)).increment(anyString());
        }
    }

    @Nested
    class AppendWarnLevelTest {
        @BeforeEach
        void init() {
            when(iLoggingEvent.getLevel()).thenReturn(Level.WARN);
            doCallRealMethod().when(appender).append(iLoggingEvent);
        }

        @Test
        void testNormal() {
            appender.append(iLoggingEvent);
            verify(multiTaggedCounter, times(1)).increment(anyString());
        }
    }
}