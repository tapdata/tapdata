package io.tapdata.websocket;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ManagementWebsocketHandlerTest {

    @Nested
    class SessionOptionTest {
        ManagementWebsocketHandler.SessionOption option;

        @BeforeEach
        void init() {
            option = mock(ManagementWebsocketHandler.SessionOption.class);
        }

        @Nested
        class ConnectTest {
            List<String> urLs;
            ManagementWebsocketHandler handler;

            @BeforeEach
            void init() {
                handler = mock(ManagementWebsocketHandler.class);
                doAnswer(w -> when(option.isOpen()).thenReturn(true)).when(handler).connect(anyString());

                urLs = new ArrayList<>();
                urLs.add("mock-url");
                when(option.isOpen()).thenReturn(false);
                doNothing().when(option).release();
                when(option.getBaseURLs()).thenReturn(urLs);
                when(option.getManagementWebsocketHandler()).thenReturn(handler);

                doCallRealMethod().when(option).connect();
            }

            void assertVerify(int openTimes, int releaseTimes, int getBaseURLsTimes, int getHandlerTimes, int connectTimes) {
                option.connect();
                verify(option, times(openTimes)).isOpen();
                verify(option, times(releaseTimes)).release();
                verify(option, times(getBaseURLsTimes)).getBaseURLs();
                verify(option, times(getHandlerTimes)).getManagementWebsocketHandler();
                verify(handler, times(connectTimes)).connect(anyString());
            }

            @Test
            void testNormal() {
                Assertions.assertDoesNotThrow(() -> assertVerify(3, 1, 1, 1, 1));
            }

            @Test
            void notOpenAtFirst() {
                when(option.isOpen()).thenReturn(true);
                Assertions.assertDoesNotThrow(() -> assertVerify(1, 0, 0, 0, 0));
            }

            @Test
            void withNullUrls() {
                when(option.getBaseURLs()).thenReturn(null);
                Assertions.assertThrows(RuntimeException.class, () -> assertVerify(1, 1, 1, 0, 0));
            }

            @Test
            void withEmptyUrls() {
                when(option.getBaseURLs()).thenReturn(new ArrayList<>());
                Assertions.assertThrows(RuntimeException.class, () -> assertVerify(1, 1, 1, 0, 0));
            }

            @Test
            void afterConnectButNotOpen() {
                doAnswer(w -> when(option.isOpen()).thenReturn(false)).when(handler).connect(anyString());
                Assertions.assertThrows(RuntimeException.class, () -> assertVerify(3, 1, 1, 1, 1));
            }
        }
    }
}