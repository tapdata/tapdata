package io.tapdata.flow.engine.V2.util;

import io.tapdata.observable.logging.ObsLogger;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class DelayHandlerTest {
    private DelayHandler delayHandler = mock(DelayHandler.class);
    @Nested
    class SleepTest{
        private ObsLogger obsLogger = mock(ObsLogger.class);
        private AtomicBoolean end = new AtomicBoolean(false);
        private String TAG = "test";
        @BeforeEach
        void beforeEach(){
            ReflectionTestUtils.setField(delayHandler,"obsLogger",obsLogger);
            ReflectionTestUtils.setField(delayHandler,"end",end);
            ReflectionTestUtils.setField(delayHandler,"TAG",TAG);
        }
        @Test
        @SneakyThrows
        @DisplayName("test sleep method when relay greater than MIN_DELAY")
        void testSleep1(){
            long relay = 20000L;
            when(delayHandler.delay()).thenReturn(relay);
            when(obsLogger.isDebugEnabled()).thenReturn(true);
            doCallRealMethod().when(delayHandler).sleep();
            delayHandler.sleep();
            verify(obsLogger, new Times(1)).debug("[{}} Successor node processing speed is limited, about to delay {} millisecond", TAG, TimeUnit.MICROSECONDS.toMillis(relay));
            verify(obsLogger, new Times(1)).info("[{}] Successor node processing speed is limited, about to delay {} millisecond", TAG, TimeUnit.MICROSECONDS.toMillis(relay));
            assertEquals(true, end.get());
        }
        @Test
        @SneakyThrows
        @DisplayName("test sleep method when relay less than MIN_DELAY")
        void testSleep2(){
            long relay = 0L;
            when(delayHandler.delay()).thenReturn(relay);
            when(obsLogger.isDebugEnabled()).thenReturn(true);
            doCallRealMethod().when(delayHandler).sleep();
            delayHandler.sleep();
            verify(obsLogger, new Times(0)).debug("[{}} Successor node processing speed is limited, about to delay {} millisecond", TAG, TimeUnit.MICROSECONDS.toMillis(relay));
            verify(obsLogger, new Times(0)).info("[{}] Successor node processing speed is limited, about to delay {} millisecond", TAG, TimeUnit.MICROSECONDS.toMillis(relay));
            assertEquals(false, end.get());
        }
        @Test
        @SneakyThrows
        @DisplayName("test sleep method when relay greater than WARN_DELAY")
        void testSleep3(){
            long relay = 20000L;
            when(delayHandler.delay()).thenReturn(relay);
            doCallRealMethod().when(delayHandler).sleep();
            delayHandler.sleep();
            verify(obsLogger, new Times(1)).info("[{}] Successor node processing speed is limited, about to delay {} millisecond", TAG, TimeUnit.MICROSECONDS.toMillis(relay));
        }
    }
    @Nested
    class DelayTest{
        @Test
        @DisplayName("test delay method when relay less than MIN_DELAY")
        void testDelay1(){
            ReflectionTestUtils.setField(delayHandler,"failed",new AtomicLong(0));
            ReflectionTestUtils.setField(delayHandler,"succeeded",new AtomicLong(1));
            doCallRealMethod().when(delayHandler).delay();
            long actual = delayHandler.delay();
            assertEquals(0, actual);
        }
        @Test
        @DisplayName("test delay method when relay greater than MAX_DELAY")
        void testDelay2(){
            ReflectionTestUtils.setField(delayHandler,"failed",new AtomicLong(20000));
            ReflectionTestUtils.setField(delayHandler,"succeeded",new AtomicLong(0));
            doCallRealMethod().when(delayHandler).delay();
            long actual = delayHandler.delay();
            assertEquals(100000, actual);
        }
    }
}
