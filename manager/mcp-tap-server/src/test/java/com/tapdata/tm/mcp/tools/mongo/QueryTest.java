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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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
class QueryTest {

    @Mock
    protected SessionAttribute sessionAttribute;

    @Mock
    protected DataSourceService dataSourceService;

    @Mock
    protected UserService userService;

    @Mock
    protected McpSyncServerExchange exchange;

    private com.tapdata.tm.mcp.tools.mongo.Query queryTool;

    @BeforeEach
    void setUp() {
        queryTool = new com.tapdata.tm.mcp.tools.mongo.Query(sessionAttribute, userService, dataSourceService);
    }

    @Test
    void testCallWithValidParameters() {
        // 准备测试数据
        String connectionId = "507f1f77bcf86cd799439011";
        String collectionName = "users";
        DataSourceConnectionDto mockConnection = new DataSourceConnectionDto();
        mockConnection.setDatabase_type("mongodb");
        UserDetail mockUserDetail = mock(UserDetail.class);

        List<Document> mockResults = Arrays.asList(
                new Document("_id", "1").append("name", "John"),
                new Document("_id", "2").append("name", "Jane")
        );

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
                when(mock.query(eq(collectionName), any())).thenReturn(mockResults);
            })) {
                // 执行测试
                Map<String, Object> params = new HashMap<>();
                params.put("connectionId", connectionId);
                params.put("collectionName", collectionName);
                params.put("filter", new Document("name", "John"));
                McpSchema.CallToolResult result = queryTool.call(exchange, params);

                // 验证结果
                MongoOperator mockMongoOperator = mc.constructed().get(0);
                assertNotNull(result);
                verify(mockMongoOperator).connect();
                verify(mockMongoOperator).query(eq(collectionName), any());
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
            assertThrows(RuntimeException.class, () -> queryTool.call(exchange, params));
        }
    }

    @Test
    void testCallWithQueryError() {
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

            // Mock MongoOperator with query error
            try (var mc = mockConstruction(MongoOperator.class, (mock, context) -> {
                when(mock.query(eq(collectionName), any())).thenThrow(new RuntimeException("Query failed"));
            })) {

                // 执行测试
                Map<String, Object> params = new HashMap<>();
                params.put("connectionId", connectionId);
                params.put("collectionName", collectionName);
                assertThrows(RuntimeException.class, () -> queryTool.call(exchange, params));

                // 验证结果
                MongoOperator mockMongoOperator = mc.constructed().get(0);
                verify(mockMongoOperator).connect();
                verify(mockMongoOperator).query(eq(collectionName), any());
            }
        }
    }
} 