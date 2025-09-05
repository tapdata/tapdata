package io.tapdata.common;

import io.tapdata.firedome.MultiTaggedCounter;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.apache.logging.log4j.Level;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
/**
 * @Author knight
 * @Date 2025/8/29 15:25
 */

class MicrometerLogAppenderTest {
    MicrometerLogAppender appender;
    MultiTaggedCounter counter;
    LogEvent event;

    @BeforeEach
    void init() {
        appender = mock(MicrometerLogAppender.class);
        counter = mock(MultiTaggedCounter.class);
        event = mock(LogEvent.class);

        ReflectionTestUtils.setField(appender, "multiTaggedCounter", counter);
    }


    @Nested
    class AppendErrorLevelTest {
        @BeforeEach
        void init() {
            when(event.getLevel()).thenReturn(Level.ERROR);
            doCallRealMethod().when(appender).append(event);
        }

        @Test
        void testNormal() {
            appender.append(event);
            verify(counter, times(1)).increment(anyString());
        }
    }

    @Nested
    class AppendWarnLevelTest {
        @BeforeEach
        void init() {
            when(event.getLevel()).thenReturn(Level.WARN);
            doCallRealMethod().when(appender).append(event);
        }

        @Test
        void testNormal() {
            appender.append(event);
            verify(counter, times(1)).increment(anyString());
        }
    }
}