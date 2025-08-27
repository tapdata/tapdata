package com.tapdata.tm.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


class HttpUtilsTest {


    @Nested
    class AccessExceptionTest {
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> HttpUtils.AccessException.throwIfNeed(true, null, null, null, null));
        }

        @Test
        void testThrow() {
            Assertions.assertThrows(HttpUtils.AccessException.class, () -> HttpUtils.AccessException.throwIfNeed(false, new RuntimeException("test"), null, null, null));
        }

        @Test
        void testNotThrow() {
            Assertions.assertDoesNotThrow(() -> HttpUtils.AccessException.throwIfNeed(true, new RuntimeException("test"), null, null, null));
        }

        @Test
        void testNotThrow2() {
            Logger logger = mock(Logger.class);
            doNothing().when(logger).error(anyString(), anyString(), anyString(), any(RuntimeException.class));
            Assertions.assertDoesNotThrow(() -> HttpUtils.AccessException.throwIfNeed(true, new RuntimeException("test"), logger, "", ""));
            verify(logger, times(1)).error(anyString(), anyString(), anyString(), any(RuntimeException.class));
        }
    }
}