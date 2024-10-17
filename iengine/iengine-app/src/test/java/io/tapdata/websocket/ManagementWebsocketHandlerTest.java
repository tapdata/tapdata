package io.tapdata.websocket;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.tm.sdk.util.Version;
import com.tapdata.tm.worker.WorkerSingletonLock;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

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

    @Nested
    class connectTest {
        ManagementWebsocketHandler managementWebsocketHandlerTest = new ManagementWebsocketHandler();

        @BeforeEach
        void init() {
            ConfigurationCenter configurationCenter = new ConfigurationCenter();
            configurationCenter.putConfig("token","test");
            configurationCenter.putConfig((ConfigurationCenter.BASR_URLS), Arrays.asList("test"));
            configurationCenter.putConfig(ConfigurationCenter.AGENT_ID,"agentId");
            configurationCenter.putConfig(ConfigurationCenter.RETRY_TIME,3);
            ReflectionTestUtils.setField(managementWebsocketHandlerTest,"agentId","test");
            ReflectionTestUtils.setField(managementWebsocketHandlerTest,"configCenter",configurationCenter);
        }
        @Test
        void connectTest_error(){
            try(MockedStatic<WorkerSingletonLock> mockedStatic = Mockito.mockStatic(WorkerSingletonLock.class);
                MockedStatic<Version> versionMockedStatic = Mockito.mockStatic(Version.class)){
                mockedStatic.when(()->WorkerSingletonLock.addTag2WsUrl(anyString())).thenReturn("ws://test:8080/ws/agent?agentId=test&access_token=test");
                versionMockedStatic.when(Version::get).thenReturn("test");
                managementWebsocketHandlerTest.connect("http://test:8080/api/");
                ListenableFuture<WebSocketSession> listenableFuture = (ListenableFuture<WebSocketSession>) ReflectionTestUtils.getField(managementWebsocketHandlerTest,"listenableFuture");
                Assertions.assertNotNull(listenableFuture);
            }
        }
        @Test
        void SessionOptionTest_Error(){
            ReflectionTestUtils.setField(managementWebsocketHandlerTest,"retryTime",2);
            ManagementWebsocketHandler.SessionOption sessionOption = managementWebsocketHandlerTest.new SessionOption();
            WebSocketSession webSocketSession = Mockito.mock(WebSocketSession.class);
            sessionOption.setSession(webSocketSession);
            TextMessage textMessage = new TextMessage("test");
            Assertions.assertThrows(RuntimeException.class,()->sessionOption.sendMessage(textMessage));
        }

        @Test
        void SessionOptionTest_Interrupt_Exception() throws ExecutionException, InterruptedException {
            ListenableFuture mockListenableFuture = mock(ListenableFuture.class);
            ReflectionTestUtils.setField(managementWebsocketHandlerTest, "listenableFuture", mockListenableFuture);
            doThrow(new InterruptedException()).when(mockListenableFuture).get();
            StandardWebSocketClient mockClient = mock(StandardWebSocketClient.class);
            when(mockClient.doHandshake(any(), any(), any(URI.class))).thenReturn(mockListenableFuture);
            Logger mockLogger = mock(Logger.class);
            ReflectionTestUtils.setField(managementWebsocketHandlerTest, "logger", mockLogger);

            ManagementWebsocketHandler spyManagementWebsocketHandlerTest = spy(managementWebsocketHandlerTest);

            try (MockedStatic<WorkerSingletonLock> mockedStatic = Mockito.mockStatic(WorkerSingletonLock.class);
                 MockedStatic<Version> versionMockedStatic = Mockito.mockStatic(Version.class);
                 MockedStatic<ManagementWebsocketHandler> managementWebsocketHandlerMockedStatic = mockStatic(ManagementWebsocketHandler.class);) {
                mockedStatic.when(() -> WorkerSingletonLock.addTag2WsUrl(anyString())).thenReturn("ws://test:8080/ws/agent?agentId=test&access_token=test");
                versionMockedStatic.when(Version::get).thenReturn("test");
                managementWebsocketHandlerMockedStatic.when(() -> ManagementWebsocketHandler.createWebSocketClient()).thenReturn(mockClient);
                String baseStr = "http://test:8080/api/";
                spyManagementWebsocketHandlerTest.connect(baseStr);
                verify(mockLogger, times(1)).warn("Connect to web socket Thread interrupted,Thread name:{}", Thread.currentThread().getName());
            }
        }

    }
}
