package com.tapdata.tm.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.userLog.service.UserLogService;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpServerSession;
import jakarta.servlet.http.HttpSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.servlet.function.ServerRequest;
import org.springframework.web.servlet.function.ServerResponse;
import org.springframework.web.servlet.function.ServerResponse.SseBuilder;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static com.tapdata.tm.mcp.SseServerTransportProvider.ENDPOINT_EVENT_TYPE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SseServerTransportProviderTest {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    private UserLogService userLogService;

    @Mock
    private ServerRequest serverRequest;

    @Mock
    private ServerRequest.Headers headers;

    @Mock
    private HttpSession session;

    @Mock
    private SseBuilder sseBuilder;

    @Mock
    private McpServerSession.Factory sessionFactory;

    @Mock
    private McpServerSession mcpServerSession;

    private SseServerTransportProvider transportProvider;

    @BeforeEach
    void setUp() {
        transportProvider = new SseServerTransportProvider(
                objectMapper,
                "/mcp/message",
                "/mcp/sse",
                userLogService
        );
        transportProvider.setSessionFactory(sessionFactory);
    }

    @Test
    void testHandleSseConnection() throws Exception {
        // 准备测试数据
        var sessionId = "test-session-id";
        when(serverRequest.session()).thenReturn(session);
        when(session.getAttribute(McpConfig.TOKEN)).thenReturn("test-token");
        when(session.getAttribute(McpConfig.USER_ID)).thenReturn("test-user");
        when(sessionFactory.create(any())).thenReturn(mcpServerSession);
        when(mcpServerSession.getId()).thenReturn(sessionId);
        when(sseBuilder.id(sessionId)).thenReturn(sseBuilder);
        when(sseBuilder.id(ENDPOINT_EVENT_TYPE)).thenReturn(sseBuilder);

        // 使用反射调用私有方法
        var method = SseServerTransportProvider.class.getDeclaredMethod("handleSseConnection", ServerRequest.class);
        method.setAccessible(true);

        ServerResponse mockServerResponse = mock(ServerResponse.class);

        try (MockedStatic<ServerResponse> mockServerResponseStatic = mockStatic(ServerResponse.class)) {
            mockServerResponseStatic.when(() -> ServerResponse.sse(any(Consumer.class), any(Duration.class)))
                    .thenAnswer(answer -> {
                        Consumer<ServerResponse.SseBuilder> consumer = answer.getArgument(0);
                        consumer.accept(sseBuilder);
                        return mockServerResponse;
                    });
            ServerResponse response = (ServerResponse) method.invoke(transportProvider, serverRequest);

            // 验证结果
            assertNotNull(response);
            verify(userLogService).addUserLog(any(), any(), anyString(), any(), any());
        }
    }

    @Test
    void testHandleMessage() throws Exception {
        // 准备测试数据
        String sessionId = "test-session-id";
        String messageBody = "{\"jsonrpc\":\"2.0\",\"method\":\"test\",\"params\":{}}";
        McpSchema.JSONRPCResponse message = new McpSchema.JSONRPCResponse("2.0", "test", Collections.emptyMap(), null);

        when(serverRequest.param("sessionId")).thenReturn(Optional.of(sessionId));
        when(serverRequest.body(String.class)).thenReturn(messageBody);

        // 注入测试会话
        Map<String, McpServerSession> sessions = (Map<String, McpServerSession>)
                ReflectionTestUtils.getField(transportProvider, "sessions");
        Assertions.assertNotNull(sessions);
        sessions.put(sessionId, mcpServerSession);

        // 注入会话属性
        Map<String, Map<String, Object>> attributes = (Map<String, Map<String, Object>>)
                ReflectionTestUtils.getField(transportProvider, "sessionAttributes");
        Assertions.assertNotNull(attributes);
        Map<String, Object> sessionAttrs = new ConcurrentHashMap<>();
        sessionAttrs.put(McpConfig.USER_ID, "test-user");
        sessionAttrs.put("sessionId", sessionId);
        attributes.put(sessionId, sessionAttrs);

        when(mcpServerSession.handle(any())).thenReturn(Mono.empty());

        // 使用反射调用私有方法
        var method = SseServerTransportProvider.class.getDeclaredMethod("handleMessage", ServerRequest.class);
        method.setAccessible(true);

        // 执行测试
        ServerResponse response = ReflectionTestUtils.invokeMethod(transportProvider, "handleMessage", serverRequest);

        // 验证结果
        assertNotNull(response);
        assertEquals(HttpStatus.OK.value(), response.statusCode().value());
        verify(mcpServerSession).handle(any());
        verify(userLogService).addUserLog(any(), any(), anyString(), any(), anyString());
    }

    @Test
    void testHandleMessageWithoutSessionId() throws Exception {
        // 准备测试数据
        when(serverRequest.param("sessionId")).thenReturn(Optional.empty());

        // 使用反射调用私有方法
        var method = SseServerTransportProvider.class.getDeclaredMethod("handleMessage", ServerRequest.class);
        method.setAccessible(true);

        // 执行测试
        ServerResponse response = (ServerResponse) method.invoke(transportProvider, serverRequest);

        // 验证结果
        assertNotNull(response);
        assertEquals(HttpStatus.BAD_REQUEST.value(), response.statusCode().value());
    }

    @Test
    void testHandleMessageWithInvalidSessionId() throws Exception {
        // 准备测试数据
        when(serverRequest.param("sessionId")).thenReturn(Optional.of("invalid-session"));

        // 使用反射调用私有方法
        var method = SseServerTransportProvider.class.getDeclaredMethod("handleMessage", ServerRequest.class);
        method.setAccessible(true);

        // 执行测试
        ServerResponse response = (ServerResponse) method.invoke(transportProvider, serverRequest);

        // 验证结果
        assertNotNull(response);
        assertEquals(HttpStatus.NOT_FOUND.value(), response.statusCode().value());
    }

    @Test
    void testNotifyClients() {
        // 准备测试数据
        String sessionId = "test-session-id";
        
        // 注入测试会话
        Map<String, McpServerSession> sessions = (Map<String, McpServerSession>) ReflectionTestUtils.getField(transportProvider, "sessions");
        sessions.put(sessionId, mcpServerSession);

        when(mcpServerSession.sendNotification(anyString(), any())).thenReturn(Mono.empty());

        // 执行测试
        Mono<Void> result = transportProvider.notifyClients("test-method", Collections.emptyMap());

        // 验证结果
        StepVerifier.create(result)
                .verifyComplete();
        verify(mcpServerSession).sendNotification(eq("test-method"), any());
    }

    @Test
    void testCloseGracefully() {
        // 准备测试数据
        String sessionId = "test-session-id";
        
        // 注入测试会话
        Map<String, McpServerSession> sessions = (Map<String, McpServerSession>) ReflectionTestUtils.getField(transportProvider, "sessions");
        sessions.put(sessionId, mcpServerSession);

        when(mcpServerSession.closeGracefully()).thenReturn(Mono.empty());

        // 执行测试
        Mono<Void> result = transportProvider.closeGracefully();

        // 验证结果
        StepVerifier.create(result)
                .verifyComplete();
        verify(mcpServerSession).closeGracefully();
    }

    @Test
    void testSessionAttributes() {
        // 准备测试数据
        String sessionId = "test-session-id";
        String key = "test-key";
        String value = "test-value";

        transportProvider.setAttribute(sessionId, key, value);
        Object retrievedValue = transportProvider.getAttribute(sessionId, key);
        Assertions.assertNull(retrievedValue);

        Map<String, Map<String, Object>> sessionAttributes =
                (Map<String, Map<String, Object>>) ReflectionTestUtils.getField(transportProvider, "sessionAttributes");
        // 初始化用户会话
        Map<String, Object> sessionAttrs = new ConcurrentHashMap<>();
        sessionAttrs.put("sessionId", "test_session_id");
        sessionAttributes.put(sessionId, sessionAttrs);
        transportProvider.setAttribute(sessionId, key, value);
        retrievedValue = transportProvider.getAttribute(sessionId, key);
        assertEquals(value, retrievedValue);

        retrievedValue = transportProvider.getAttribute("test_session_id", key);
        assertEquals(value, retrievedValue);

        // 验证获取所有属性
        Map<String, Object> attributes = transportProvider.getSessionAttributes(sessionId);
        assertNotNull(attributes);
        assertEquals(value, attributes.get(key));
    }

    @Test
    void testGetSessionAttributesWithInvalidSession() {
        // 测试无效会话ID
        Map<String, Object> attributes = transportProvider.getSessionAttributes("invalid-session");
        assertNotNull(attributes);
        assertTrue(attributes.isEmpty());
    }

    @Test
    void testWebMvcMcpSessionTransport() throws Exception {
        // 准备测试数据
        String sessionId = "test-session-id";
        McpSchema.JSONRPCMessage message = new McpSchema.JSONRPCRequest("2.0", "test", Collections.emptyMap(), null);
        String messageJson = "{\"jsonrpc\":\"2.0\",\"method\":\"test\",\"id\":{}}";

        when(sseBuilder.id(anyString())).thenReturn(sseBuilder);
        when(sseBuilder.event(anyString())).thenReturn(sseBuilder);

        // 创建 WebMvcMcpSessionTransport 实例
        var constructor = SseServerTransportProvider.class.getDeclaredClasses()[0].getDeclaredConstructor(
                SseServerTransportProvider.class, String.class, SseBuilder.class);
        constructor.setAccessible(true);
        var transport = constructor.newInstance(transportProvider, sessionId, sseBuilder);

        // 执行测试
        Mono<Void> result = ReflectionTestUtils.invokeMethod(transport, "sendMessage", message);

        // 验证结果
        Assertions.assertNotNull(result);
        StepVerifier.create(result).verifyComplete();
        verify(sseBuilder).id(sessionId);
        verify(sseBuilder).event(SseServerTransportProvider.MESSAGE_EVENT_TYPE);
        verify(sseBuilder).data(messageJson);
    }
} 