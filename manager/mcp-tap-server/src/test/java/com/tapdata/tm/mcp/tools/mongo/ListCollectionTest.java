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
class ListCollectionTest {
    @Mock
    protected SessionAttribute sessionAttribute;

    @Mock
    protected DataSourceService dataSourceService;

    @Mock
    protected UserService userService;

    @Mock
    protected McpSyncRequestContext context;

    private ListCollection listCollection;

    @BeforeEach
    void setUp() {
        McpToolSupport toolSupport = new McpToolSupport(sessionAttribute, userService);
        listCollection = new ListCollection(new MongoOperatorFactory(toolSupport, dataSourceService));
    }

    @Test
    void testCallWithNameOnlyTrue() {
        // 准备测试数据
        String connectionId = "507f1f77bcf86cd799439011";
        DataSourceConnectionDto mockConnection = new DataSourceConnectionDto();
        mockConnection.setDatabase_type("mongodb");
        UserDetail mockUserDetail = mock(UserDetail.class);

        List mockCollections = Arrays.asList("collection1", "collection2");
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(dataSourceService.findOne(any(Query.class), eq(mockUserDetail))).thenReturn(mockConnection);

            // Mock MongoOperator
            try (var mc = mockConstruction(MongoOperator.class, (mock, context) -> {
                when(mock.listCollections(true)).thenReturn(mockCollections);
            })) {
                // 执行测试
                List<Object> result = listCollection.listCollection(context, connectionId, true);

                // 验证结果
                var mockMongoOperator = mc.constructed().get(0);
                assertNotNull(result);
                assertEquals(mockCollections, result);
                verify(mockMongoOperator).connect();
                verify(mockMongoOperator).listCollections(true);
            }
        }
    }

    @Test
    void testCallWithNameOnlyFalse() {
        // 准备测试数据
        String connectionId = "507f1f77bcf86cd799439011";
        DataSourceConnectionDto mockConnection = new DataSourceConnectionDto();
        mockConnection.setDatabase_type("mongodb");
        UserDetail mockUserDetail = mock(UserDetail.class);

        List mockCollections = Arrays.asList(
                new Document("name", "collection1").append("type", "collection"),
                new Document("name", "collection2").append("type", "collection")
        );
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(dataSourceService.findOne(any(Query.class), eq(mockUserDetail))).thenReturn(mockConnection);

            // Mock MongoOperator
            try (var mc = mockConstruction(MongoOperator.class, (mock, context) -> {
                when(mock.listCollections(false)).thenReturn(mockCollections);
            })) {

                // 执行测试
                List<Object> result = listCollection.listCollection(context, connectionId, false);

                // 验证结果
                assertNotNull(result);
                assertEquals(mockCollections, result);
                var mockMongoOperator = mc.constructed().get(0);
                verify(mockMongoOperator).connect();
                verify(mockMongoOperator).listCollections(false);
            }
        }
    }

    @Test
    void testCallWithConnectionError() {
        // 准备测试数据
        String connectionId = "507f1f77bcf86cd799439011";
        DataSourceConnectionDto mockConnection = new DataSourceConnectionDto();
        mockConnection.setDatabase_type("mongodb");
        UserDetail mockUserDetail = mock(UserDetail.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(dataSourceService.findOne(any(Query.class), eq(mockUserDetail))).thenReturn(mockConnection);

            // Mock MongoOperator with connection error
            try (var mc = mockConstruction(MongoOperator.class, (mock, context) -> {
                doThrow(new RuntimeException("Connection failed")).when(mock).connect();
            })) {

                // 执行测试
                assertThrows(RuntimeException.class, () -> listCollection.listCollection(context, connectionId, null));

                // 验证结果
                var mockMongoOperator = mc.constructed().get(0);
                verify(mockMongoOperator).connect();
            }
        }
    }
}
