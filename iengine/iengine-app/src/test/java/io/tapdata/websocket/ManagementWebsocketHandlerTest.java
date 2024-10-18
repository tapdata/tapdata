package io.tapdata.websocket;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.JSONUtil;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.sdk.util.Version;
import com.tapdata.tm.worker.WorkerSingletonLock;
import io.tapdata.common.SettingService;
import io.tapdata.flow.engine.V2.task.TaskService;
import io.tapdata.websocket.handler.BaseEventHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;

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
    }

    @Nested
    public class TestHandleMessage {
        public ManagementWebsocketHandler beforeEach() {
            ManagementWebsocketHandler handler = new ManagementWebsocketHandler();
            ThreadPoolExecutor threadPool = mock(ThreadPoolExecutor.class);
            doAnswer(answer -> {
                Runnable runnable = answer.getArgument(0);
                runnable.run();
                return null;
            }).when(threadPool).execute(any(Runnable.class));
            ReflectionTestUtils.setField(handler, "websocketHandleMessageThreadPoolExecutor", threadPool);

            Set<BeanDefinition> fileDetectorDefinition = new HashSet<>();
            ReflectionTestUtils.setField(handler, "fileDetectorDefinition", fileDetectorDefinition);

            BeanDefinition beanDefinition = new GenericBeanDefinition();
            beanDefinition.setBeanClassName("io.tapdata.websocket.handler.PingEventHandler");
            fileDetectorDefinition.add(beanDefinition);

            beanDefinition = new GenericBeanDefinition();
            beanDefinition.setBeanClassName("io.tapdata.websocket.handler.MockEventHandler");
            fileDetectorDefinition.add(beanDefinition);

            ManagementWebsocketHandler.SessionOption sessionOption = mock(ManagementWebsocketHandler.SessionOption.class);
            ReflectionTestUtils.setField(handler, "session", sessionOption);

            return handler;
        }

        @Test
        public void testHandleUnknownMessage() throws Exception {
            ManagementWebsocketHandler handler = beforeEach();
            WebSocketMessage message = mock(WebSocketMessage.class);

            Map<String, Object> messageData = new HashMap<>();
            messageData.put("type", "test");

            Map<String, Object> messageBody = new HashMap<>();
            messageBody.put("type", "test");
            messageBody.put("data", messageData);

            when(message.getPayload()).thenReturn(JSONUtil.obj2Json(messageBody));
            handler.handleMessage(null, message);
        }

        @Test
        public void testHandlePingMessage() throws Exception {
            ManagementWebsocketHandler handler = beforeEach();
            WebSocketMessage message = mock(WebSocketMessage.class);

            Map<String, Object> messageData = new HashMap<>();
            messageData.put("type", "ping");

            Map<String, Object> messageBody = new HashMap<>();
            messageBody.put("type", "ping");
            messageBody.put("data", messageData);

            when(message.getPayload()).thenReturn(JSONUtil.obj2Json(messageBody));
            handler.handleMessage(null, message);
        }
    }

}
