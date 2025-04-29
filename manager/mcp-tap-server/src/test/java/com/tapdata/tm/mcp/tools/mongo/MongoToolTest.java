package com.tapdata.tm.mcp.tools.mongo;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.mcp.Utils;
import com.tapdata.tm.mcp.mongodb.MongoOperator;
import com.tapdata.tm.user.service.UserService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpServerSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.query.Query;

import java.util.HashMap;
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
class MongoToolTest {

    @Mock
    protected SessionAttribute sessionAttribute;

    @Mock
    protected DataSourceService dataSourceService;

    @Mock
    protected UserService userService;

    @Mock
    protected McpSyncServerExchange exchange;

    private TestMongoTool mongoTool;

    @BeforeEach
    void setUp() {
        mongoTool = new TestMongoTool(sessionAttribute, userService, dataSourceService);
    }

    @Test
    void testCreateMongoClientWithValidConnection() {
        // 准备测试数据
        String connectionId = "507f1f77bcf86cd799439011";
        DataSourceConnectionDto mockConnection = new DataSourceConnectionDto();
        mockConnection.setDatabase_type("mongodb");
        UserDetail mockUserDetail = mock(UserDetail.class);

        McpServerSession mockSession = mock(McpServerSession.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(dataSourceService.findOne(any(Query.class), eq(mockUserDetail))).thenReturn(mockConnection);

            // 执行测试
            Map<String, Object> params = new HashMap<>();
            params.put("connectionId", connectionId);
            MongoOperator operator = mongoTool.createMongoClient(exchange, params);

            // 验证结果
            assertNotNull(operator);
            verify(dataSourceService).findOne(any(Query.class), eq(mockUserDetail));
        }
    }

    @Test
    void testCreateMongoClientWithoutConnectionId() {
        McpServerSession mockSession = mock(McpServerSession.class);
        UserDetail mockUserDetail = mock(UserDetail.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);

            // 执行测试
            Map<String, Object> params = new HashMap<>();
            assertThrows(RuntimeException.class, () -> mongoTool.createMongoClient(exchange, params));
        }
    }

    @Test
    void testCreateMongoClientNotMongoDBConnection() {
        McpServerSession mockSession = mock(McpServerSession.class);
        UserDetail mockUserDetail = mock(UserDetail.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(dataSourceService.findOne(any(Query.class), eq(mockUserDetail))).thenReturn(mock(DataSourceConnectionDto.class));

            // 执行测试
            Map<String, Object> params = new HashMap<>();
            params.put("connectionId", "507f1f77bcf86cd799439011");
            assertThrows(RuntimeException.class, () -> mongoTool.createMongoClient(exchange, params));
        }
    }

    @Test
    void testCreateMongoClientWithNonMongoConnection() {
        // 准备测试数据
        String connectionId = "507f1f77bcf86cd799439011";
        DataSourceConnectionDto mockConnection = new DataSourceConnectionDto();
        mockConnection.setDatabase_type("mysql");
        UserDetail mockUserDetail = mock(UserDetail.class);

        McpServerSession mockSession = mock(McpServerSession.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);

            // 执行测试
            Map<String, Object> params = new HashMap<>();
            params.put("connectionId", connectionId);
            assertThrows(RuntimeException.class, () -> mongoTool.createMongoClient(exchange, params));
        }
    }

    @Test
    void testCreateMongoClientWithNonExistentConnection() {
        // 准备测试数据
        String connectionId = "507f1f77bcf86cd799439011";
        UserDetail mockUserDetail = mock(UserDetail.class);

        McpServerSession mockSession = mock(McpServerSession.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(dataSourceService.findOne(any(Query.class), eq(mockUserDetail))).thenReturn(null);

            // 执行测试
            Map<String, Object> params = new HashMap<>();
            params.put("connectionId", connectionId);
            assertThrows(RuntimeException.class, () -> mongoTool.createMongoClient(exchange, params));
        }
    }

    private static class TestMongoTool extends MongoTool {
        public TestMongoTool(SessionAttribute sessionAttribute, UserService userService, DataSourceService dataSourceService) {
            super("test", "test", "{}", sessionAttribute, userService, dataSourceService);
        }

        @Override
        public McpSchema.CallToolResult call(McpSyncServerExchange exchange, Map<String, Object> params) {
            return null;
        }
    }
} 