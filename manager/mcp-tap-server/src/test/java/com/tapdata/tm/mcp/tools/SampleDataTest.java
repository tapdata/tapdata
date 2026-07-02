package com.tapdata.tm.mcp.tools;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.mcp.Utils;
import com.tapdata.tm.user.service.UserService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.mcp.annotation.context.McpSyncRequestContext;
import org.springframework.boot.web.server.servlet.context.ServletWebServerApplicationContext;
import org.springframework.boot.web.server.WebServer;

import java.io.IOException;
import java.util.Map;

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
    private McpSyncRequestContext context;

    @Mock
    private WebServer webServer;

    private SampleData sampleData;

    @BeforeEach
    void setUp() {
        when(webServerAppCtxt.getWebServer()).thenReturn(webServer);
        when(webServer.getPort()).thenReturn(8080);
        sampleData = new SampleData(new McpToolSupport(sessionAttribute, userService), webServerAppCtxt);
    }
    @Test
    void testCallWithValidParameters() {
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            ms.when(() -> Utils.parseJson(any(), any(TypeReference.class))).thenCallRealMethod();
            ms.when(() -> Utils.parseJson(any(), any(Class.class))).thenCallRealMethod();
            ms.when(() -> Utils.sendPostRequest(any(String.class), any(Map.class)))
                    .thenReturn("""
                            {"code": "ok", "data": [{"test": "test"}]}""");
            // 执行测试
            Object result = sampleData.sampleData(context, "507f1f77bcf86cd799439011", "test_schema");
            assertNotNull(result);
        }
    }

    @Test
    void testCallWithInvalidParameters() {
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            ms.when(() -> Utils.sendPostRequest(any(String.class), any(Map.class)))
                    .thenReturn("""
                            {"code": "ok", "data": [{"test": "test"}]}""");
            // 执行测试
            assertThrows(RuntimeException.class,
                    () -> sampleData.sampleData(context, "507f1f77bcf86cd799439011", "test_schema"));
        }
    }

    @Test
    void testCallWithIOErrorParameters() {
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            ms.when(() -> Utils.sendPostRequest(any(String.class), any(Map.class))).thenThrow(new IOException("connect server timeout"));
            // 执行测试
            assertThrows(RuntimeException.class,
                    () -> sampleData.sampleData(context, "507f1f77bcf86cd799439011", "test_schema"));
        }
    }

    @Test
    void testCallWithoutConnectionId() {
        // 执行测试
        // 验证异常
        assertThrows(RuntimeException.class, () -> sampleData.sampleData(context, null, "test_schema"));
    }

    @Test
    void testCallWithoutSchemaName() {
        // 执行测试
        // 验证异常
        assertThrows(RuntimeException.class, () -> sampleData.sampleData(context, "507f1f77bcf86cd799439011", null));
    }

    @Test
    void testCallWithInvalidSession() {
        // 执行测试
        // 验证异常
        assertThrows(RuntimeException.class,
                () -> sampleData.sampleData(context, "507f1f77bcf86cd799439011", "test_schema"));
    }
}
