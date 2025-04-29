package com.tapdata.tm.mcp.tools;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.mcp.Utils;
import com.tapdata.tm.user.service.UserService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpServerSession;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.boot.web.server.WebServer;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * @date 2025/04/21 09:08
 */
@ExtendWith(MockitoExtension.class)
class SampleDataTest {

    @Mock
    private SessionAttribute sessionAttribute;

    @Mock
    private UserService userService;

    @Mock
    private ServletWebServerApplicationContext webServerAppCtxt;

    @Mock
    private McpSyncServerExchange exchange;

    @Mock
    private WebServer webServer;

    private SampleData sampleData;

    @BeforeEach
    void setUp() {
        when(webServerAppCtxt.getWebServer()).thenReturn(webServer);
        when(webServer.getPort()).thenReturn(8080);
        sampleData = new SampleData(sessionAttribute, userService, webServerAppCtxt);
    }
    @Test
    void testCallWithValidParameters() {
        McpServerSession mockSession = mock(McpServerSession.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            ms.when(() -> Utils.parseJson(any(), any(TypeReference.class))).thenCallRealMethod();
            ms.when(() -> Utils.parseJson(any(), any(Class.class))).thenCallRealMethod();
            ms.when(() -> Utils.sendPostRequest(any(String.class), any(Map.class)))
                    .thenReturn("""
                            {"code": "ok", "data": [{"test": "test"}]}""");
            // 执行测试
            Map<String, Object> params = new HashMap<>();
            params.put("connectionId", "507f1f77bcf86cd799439011");
            params.put("schemaName", "test_schema");
            McpSchema.CallToolResult result = sampleData.call(exchange, params);
            assertNotNull(result);
        }
    }

    @Test
    void testCallWithInvalidParameters() {
        McpServerSession mockSession = mock(McpServerSession.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            ms.when(() -> Utils.sendPostRequest(any(String.class), any(Map.class)))
                    .thenReturn("""
                            {"code": "ok", "data": [{"test": "test"}]}""");
            // 执行测试
            Map<String, Object> params = new HashMap<>();
            params.put("connectionId", "507f1f77bcf86cd799439011");
            params.put("schemaName", "test_schema");
            assertThrows(RuntimeException.class, () -> sampleData.call(exchange, params));
        }
    }

    @Test
    void testCallWithIOErrorParameters() {
        McpServerSession mockSession = mock(McpServerSession.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            ms.when(() -> Utils.sendPostRequest(any(String.class), any(Map.class))).thenThrow(new IOException("connect server timeout"));
            // 执行测试
            Map<String, Object> params = new HashMap<>();
            params.put("connectionId", "507f1f77bcf86cd799439011");
            params.put("schemaName", "test_schema");
            assertThrows(RuntimeException.class, () -> sampleData.call(exchange, params));
        }
    }

    @Test
    void testCallWithoutConnectionId() {
        // 执行测试
        Map<String, Object> params = new HashMap<>();
        params.put("schemaName", "test_schema");

        // 验证异常
        assertThrows(RuntimeException.class, () -> sampleData.call(exchange, params));
    }

    @Test
    void testCallWithoutSchemaName() {
        // 执行测试
        Map<String, Object> params = new HashMap<>();
        params.put("connectionId", "507f1f77bcf86cd799439011");

        // 验证异常
        assertThrows(RuntimeException.class, () -> sampleData.call(exchange, params));
    }

    @Test
    void testCallWithInvalidSession() {
        // 执行测试
        Map<String, Object> params = new HashMap<>();
        params.put("connectionId", "507f1f77bcf86cd799439011");
        params.put("schemaName", "test_schema");

        // 验证异常
        assertThrows(RuntimeException.class, () -> sampleData.call(exchange, params));
    }

    @Test
    void testGetUserDetail() {
        McpServerSession mockSession = mock(McpServerSession.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);

            ReflectionTestUtils.setField(sampleData, "sessionAttribute", null);
            assertThrows(RuntimeException.class, () -> sampleData.getUserDetail(exchange));

            ReflectionTestUtils.setField(sampleData, "sessionAttribute", sessionAttribute);
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn(null);
            assertThrows(RuntimeException.class, () -> sampleData.getUserDetail(exchange));

            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn(new ObjectId().toHexString());
            ReflectionTestUtils.setField(sampleData, "userService", null);
            assertThrows(RuntimeException.class, () -> sampleData.getUserDetail(exchange));

        }
    }
} 