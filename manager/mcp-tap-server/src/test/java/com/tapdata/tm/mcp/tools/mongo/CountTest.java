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
import org.bson.Document;
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
class CountTest {

    @Mock
    protected SessionAttribute sessionAttribute;

    @Mock
    protected DataSourceService dataSourceService;

    @Mock
    protected UserService userService;

    @Mock
    protected McpSyncServerExchange exchange;

    private Count countTool;

    @BeforeEach
    void setUp() {
        countTool = new Count(sessionAttribute, userService, dataSourceService);
    }

    @Test
    void testCallWithValidParameters() {
        // 准备测试数据
        String connectionId = "507f1f77bcf86cd799439011";
        String collectionName = "users";
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

            // Mock MongoOperator
            try (var mc = mockConstruction(MongoOperator.class, (mock, context) -> {
                when(mock.count(eq(collectionName), any())).thenReturn(100L);
            })) {
                // 执行测试
                Map<String, Object> params = new HashMap<>();
                params.put("connectionId", connectionId);
                params.put("collectionName", collectionName);
                params.put("filter", new Document("age", new Document("$gt", 18)));
                McpSchema.CallToolResult result = countTool.call(exchange, params);

                // 验证结果
                var mockMongoOperator = mc.constructed().get(0);
                assertNotNull(result);
                verify(mockMongoOperator).connect();
                verify(mockMongoOperator).count(eq(collectionName), any());
            }
        }
    }

    @Test
    void testCallWithoutCollectionName() {
        // 准备测试数据
        String connectionId = "507f1f77bcf86cd799439011";
        DataSourceConnectionDto mockConnection = new DataSourceConnectionDto();
        mockConnection.setDatabase_type("mongodb");

        McpServerSession mockSession = mock(McpServerSession.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();

            // 执行测试
            Map<String, Object> params = new HashMap<>();
            params.put("connectionId", connectionId);
            assertThrows(RuntimeException.class, () -> countTool.call(exchange, params));
        }
    }

    @Test
    void testCallWithCountError() {
        // 准备测试数据
        String connectionId = "507f1f77bcf86cd799439011";
        String collectionName = "users";
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

            // Mock MongoOperator with count error
            try (var mc = mockConstruction(MongoOperator.class, (mock, context) -> {
                when(mock.count(eq(collectionName), any()))
                        .thenThrow(new RuntimeException("Count failed"));
            })) {

                // 执行测试
                Map<String, Object> params = new HashMap<>();
                params.put("connectionId", connectionId);
                params.put("collectionName", collectionName);
                assertThrows(RuntimeException.class, () -> countTool.call(exchange, params));

                // 验证结果
                MongoOperator mockMongoOperator = mc.constructed().get(0);
                verify(mockMongoOperator).connect();
                verify(mockMongoOperator).count(eq(collectionName), any());
            }
        }
    }

    @Test
    void testCallWithZeroCount() {
        // 准备测试数据
        String connectionId = "507f1f77bcf86cd799439011";
        String collectionName = "users";
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

            // Mock MongoOperator
            try (var mc = mockConstruction(MongoOperator.class, (mock, context) -> {
                when(mock.count(eq(collectionName), any())).thenReturn(0L);
            })) {

                // 执行测试
                Map<String, Object> params = new HashMap<>();
                params.put("connectionId", connectionId);
                params.put("collectionName", collectionName);
                McpSchema.CallToolResult result = countTool.call(exchange, params);

                // 验证结果
                MongoOperator mockMongoOperator = mc.constructed().get(0);
                assertNotNull(result);
                verify(mockMongoOperator).connect();
                verify(mockMongoOperator).count(eq(collectionName), any());
            }
        }
    }
} 