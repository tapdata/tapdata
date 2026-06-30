package com.tapdata.tm.mcp.tools.mongo;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.mcp.Utils;
import com.tapdata.tm.mcp.mongodb.MongoOperator;
import com.tapdata.tm.mcp.tools.McpToolSupport;
import com.tapdata.tm.user.service.UserService;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.mcp.annotation.context.McpSyncRequestContext;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * @date 2025/04/21 09:08
 */
@ExtendWith(MockitoExtension.class)
class AggregateTest {
    @Mock
    protected SessionAttribute sessionAttribute;

    @Mock
    protected DataSourceService dataSourceService;

    @Mock
    protected UserService userService;

    @Mock
    protected McpSyncRequestContext context;

    private Aggregate aggregateTool;

    @BeforeEach
    void setUp() {
        McpToolSupport toolSupport = new McpToolSupport(sessionAttribute, userService);
        aggregateTool = new Aggregate(new MongoOperatorFactory(toolSupport, dataSourceService));
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
                new Document("_id", "group1").append("count", 10),
                new Document("_id", "group2").append("count", 20)
        );
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(dataSourceService.findOne(any(Query.class), eq(mockUserDetail))).thenReturn(mockConnection);

            // Mock MongoOperator
            try (var mc = mockConstruction(MongoOperator.class,  (mock, context) -> {
                when(mock.aggregate(eq(collectionName), any())).thenReturn(mockResults);
            })) {

                // 执行测试
                List<Document> result = aggregateTool.aggregate(context, connectionId, collectionName,
                        Arrays.asList(new Document("$group", new Document("_id", "$category").append("count", new Document("$sum", 1)))),
                        null);

                // 验证结果
                MongoOperator mockMongoOperator = mc.constructed().get(0);
                assertNotNull(result);
                assertEquals(mockResults, result);
                verify(mockMongoOperator).connect();
                verify(mockMongoOperator).aggregate(eq(collectionName), any());
            }
        }
    }

    @Test
    void testCallWithoutCollectionName() {
        // 准备测试数据
        String connectionId = "507f1f77bcf86cd799439011";
        DataSourceConnectionDto mockConnection = new DataSourceConnectionDto();
        mockConnection.setDatabase_type("mongodb");
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();

            // 执行测试
            assertThrows(RuntimeException.class,
                    () -> aggregateTool.aggregate(context, connectionId, null, null, null));
        }
    }

    @Test
    void testCallWithAggregateError() {
        // 准备测试数据
        String connectionId = "507f1f77bcf86cd799439011";
        String collectionName = "users";
        DataSourceConnectionDto mockConnection = new DataSourceConnectionDto();
        mockConnection.setDatabase_type("mongodb");
        UserDetail mockUserDetail = mock(UserDetail.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(dataSourceService.findOne(any(Query.class), eq(mockUserDetail))).thenReturn(mockConnection);

            // Mock MongoOperator with aggregate error
            try (var mc = mockConstruction(MongoOperator.class, (mock, context) -> {
                when(mock.aggregate(eq(collectionName), any())).thenThrow(new RuntimeException("Aggregate failed"));
            })) {

                // 执行测试
                assertThrows(RuntimeException.class,
                        () -> aggregateTool.aggregate(context, connectionId, collectionName, Arrays.asList(new Document("$invalid", 1)), null));

                // 验证结果
                MongoOperator mockMongoOperator = mc.constructed().get(0);
                verify(mockMongoOperator).connect();
                verify(mockMongoOperator).aggregate(eq(collectionName), any());
            }
        }
    }
}
