package com.tapdata.tm.ws.endpoint;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.web.socket.WebSocketSession;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WebSocketManagerTest {
    @Nested
    class formatMessageIfNeed{
        String message;
        WebSocketSession session;
        @BeforeEach
        void beforeEach(){
            message = "{\"type\":\"pipe\",\"data\":{\"type\":\"testConnectionResult\",\"status\":\"SUCCESS\",\"result\":{\"response_body\":{\"validate_details\":[{\"show_msg\":\"PDK Connector version\",\"status\":\"passed\",\"sort\":0,\"fail_message\":\" (build: 2024-08-06 10:23:26)\",\"required\":true,\"cost\":0},{\"show_msg\":\"Check host port is valid\",\"status\":\"failed\",\"sort\":0,\"item_exception\":{\"stackTrace\":[{\"methodName\":\"testHostPort\",\"fileName\":\"CommonDbTest.java\",\"lineNumber\":74,\"className\":\"io.tapdata.common.CommonDbTest\",\"nativeMethod\":false},{\"methodName\":\"testOneByOne\",\"fileName\":\"CommonDbTest.java\",\"lineNumber\":58,\"className\":\"io.tapdata.common.CommonDbTest\",\"nativeMethod\":false},{\"methodName\":\"connectionTest\",\"fileName\":\"MysqlConnector.java\",\"lineNumber\":696,\"className\":\"io.tapdata.connector.mysql.MysqlConnector\",\"nativeMethod\":false},{\"methodName\":\"connectionTest\",\"fileName\":\"ConnectionNode.java\",\"lineNumber\":45,\"className\":\"io.tapdata.pdk.core.api.ConnectionNode\",\"nativeMethod\":false},{\"methodName\":\"lambda$testPdkConnection$2\",\"fileName\":\"ConnectionValidator.java\",\"lineNumber\":206,\"className\":\"com.tapdata.validator.ConnectionValidator\",\"nativeMethod\":false},{\"methodName\":\"invokePDKMethodPrivate\",\"fileName\":\"PDKInvocationMonitor.java\",\"lineNumber\":164,\"className\":\"io.tapdata.pdk.core.monitor.PDKInvocationMonitor\",\"nativeMethod\":false},{\"methodName\":\"lambda$invokePDKMethod$5\",\"fileName\":\"PDKInvocationMonitor.java\",\"lineNumber\":124,\"className\":\"io.tapdata.pdk.core.monitor.PDKInvocationMonitor\",\"nativeMethod\":false},{\"methodName\":\"applyClassLoaderContext\",\"fileName\":\"Node.java\",\"lineNumber\":27,\"className\":\"io.tapdata.pdk.core.api.Node\",\"nativeMethod\":false},{\"methodName\":\"invokePDKMethod\",\"fileName\":\"PDKInvocationMonitor.java\",\"lineNumber\":124,\"className\":\"io.tapdata.pdk.core.monitor.PDKInvocationMonitor\",\"nativeMethod\":false},{\"methodName\":\"invokePDKMethod\",\"fileName\":\"PDKInvocationMonitor.java\",\"lineNumber\":108,\"className\":\"io.tapdata.pdk.core.monitor.PDKInvocationMonitor\",\"nativeMethod\":false},{\"methodName\":\"invoke\",\"fileName\":\"PDKInvocationMonitor.java\",\"lineNumber\":85,\"className\":\"io.tapdata.pdk.core.monitor.PDKInvocationMonitor\",\"nativeMethod\":false},{\"methodName\":\"testPdkConnection\",\"fileName\":\"ConnectionValidator.java\",\"lineNumber\":205,\"className\":\"com.tapdata.validator.ConnectionValidator\",\"nativeMethod\":false},{\"methodName\":\"testPdkConnection\",\"fileName\":\"TestConnectionHandler.java\",\"lineNumber\":247,\"className\":\"io.tapdata.websocket.handler.TestConnectionHandler\",\"nativeMethod\":false},{\"methodName\":\"handleSync\",\"fileName\":\"TestConnectionHandler.java\",\"lineNumber\":153,\"className\":\"io.tapdata.websocket.handler.TestConnectionHandler\",\"nativeMethod\":false},{\"methodName\":\"lambda$handle$0\",\"fileName\":\"TestConnectionHandler.java\",\"lineNumber\":124,\"className\":\"io.tapdata.websocket.handler.TestConnectionHandler\",\"nativeMethod\":false},{\"methodName\":\"lambda$aspectRunnable$1\",\"fileName\":\"AspectRunnableUtil.java\",\"lineNumber\":11,\"className\":\"io.tapdata.aspect.supervisor.AspectRunnableUtil\",\"nativeMethod\":false},{\"methodName\":\"run\",\"fileName\":\"Thread.java\",\"lineNumber\":748,\"className\":\"java.lang.Thread\",\"nativeMethod\":false}],\"message\":\"check.host.port.fail\",\"reason\":\"check.host.port.reason\",\"stack\":\"java.io.IOException: Unable connect to 127.0.0.1:3307, reason: Connection refused (Connection refused)\\n\\tat io.tapdata.util.NetUtil.validateHostPortWithSocket(NetUtil.java:37)\\n\\tat io.tapdata.util.NetUtil.validateHostPortWithSocket(NetUtil.java:42)\\n\\tat io.tapdata.common.CommonDbTest.testHostPort(CommonDbTest.java:69)\\n\\tat io.tapdata.common.CommonDbTest.testOneByOne(CommonDbTest.java:58)\\n\\tat io.tapdata.connector.mysql.MysqlConnector.connectionTest(MysqlConnector.java:696)\\n\\tat io.tapdata.pdk.core.api.ConnectionNode.connectionTest(ConnectionNode.java:45)\\n\\tat com.tapdata.validator.ConnectionValidator.lambda$testPdkConnection$2(ConnectionValidator.java:206)\\n\\tat io.tapdata.pdk.core.monitor.PDKInvocationMonitor.invokePDKMethodPrivate(PDKInvocationMonitor.java:164)\\n\\tat io.tapdata.pdk.core.monitor.PDKInvocationMonitor.lambda$invokePDKMethod$5(PDKInvocationMonitor.java:124)\\n\\tat io.tapdata.pdk.core.api.Node.applyClassLoaderContext(Node.java:27)\\n\\tat io.tapdata.pdk.core.monitor.PDKInvocationMonitor.invokePDKMethod(PDKInvocationMonitor.java:124)\\n\\tat io.tapdata.pdk.core.monitor.PDKInvocationMonitor.invokePDKMethod(PDKInvocationMonitor.java:108)\\n\\tat io.tapdata.pdk.core.monitor.PDKInvocationMonitor.invoke(PDKInvocationMonitor.java:85)\\n\\tat com.tapdata.validator.ConnectionValidator.testPdkConnection(ConnectionValidator.java:205)\\n\\tat io.tapdata.websocket.handler.TestConnectionHandler.testPdkConnection(TestConnectionHandler.java:247)\\n\\tat io.tapdata.websocket.handler.TestConnectionHandler.handleSync(TestConnectionHandler.java:153)\\n\\tat io.tapdata.websocket.handler.TestConnectionHandler.lambda$handle$0(TestConnectionHandler.java:124)\\n\\tat io.tapdata.aspect.supervisor.AspectRunnableUtil.lambda$aspectRunnable$1(AspectRunnableUtil.java:11)\\n\\tat java.lang.Thread.run(Thread.java:748)\\nCaused by: java.net.ConnectException: Connection refused (Connection refused)\\n\\tat java.net.PlainSocketImpl.socketConnect(Native Method)\\n\\tat java.net.AbstractPlainSocketImpl.doConnect(AbstractPlainSocketImpl.java:350)\\n\\tat java.net.AbstractPlainSocketImpl.connectToAddress(AbstractPlainSocketImpl.java:206)\\n\\tat java.net.AbstractPlainSocketImpl.connect(AbstractPlainSocketImpl.java:188)\\n\\tat java.net.SocksSocketImpl.connect(SocksSocketImpl.java:392)\\n\\tat java.net.Socket.connect(Socket.java:589)\\n\\tat io.tapdata.util.NetUtil.validateHostPortWithSocket(NetUtil.java:34)\\n\\t... 18 more\\n\",\"solution\":\"check.host.port.solution\",\"host\":\"127.0.0.1\",\"port\":\"3307\",\"localizedMessage\":\"check.host.port.fail\",\"suppressed\":[]},\"required\":true,\"cost\":0}]},\"id\":\"66ab591b77fe802bff234992\",\"status\":\"invalid\"}},\"sender\":\"kiki_iengine\",\"receiver\":\"89f528e4-e642-56c7-4319-14632e75a989\"}";
            session = mock(WebSocketSession.class);
        }
        @Test
        @DisplayName("test formatMessageIfNeed method when message is blank")
        void test1(){
            message = "";
            String actual = WebSocketManager.formatMessageIfNeed(message, session);
            assertEquals(message, actual);

        }
        @Test
        @DisplayName("test formatMessageIfNeed method when message contains testConnectionResult")
        void test2(){
            HttpHeaders headers = mock(HttpHeaders.class);
            when(session.getHandshakeHeaders()).thenReturn(headers);
            List<String> cookieString = new ArrayList<>();
            String cookie = "ken_bd_vid=kennen; access_token=ae871e4e6c8f4a1bb3a5ebce30d36134409a0577f1574b27a9f7f6037545d954; tem_token=ae871e4e6c8f4a1bb3a5ebce30d36134409a0577f1574b27a9f7f6037545d954; email=admin@admin.com; username=; isAdmin=1; user_id=62bc5008d4958d013d97c7a6; lang=zh_CN";
            cookieString.add(cookie);
            when(headers.get("cookie")).thenReturn(cookieString);
            String actual = WebSocketManager.formatMessageIfNeed(message, session);
            assertTrue(actual.contains("连接主机和端口失败"));
        }
    }
}
