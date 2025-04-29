package com.tapdata.tm.mcp.resource;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.bean.Schema;
import com.tapdata.tm.commons.schema.bean.Table;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.service.impl.DataSourceService;
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
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * @date 2025/04/21 09:08
 */
@ExtendWith(MockitoExtension.class)
class ConnectionResourceTest {

    @Mock
    private SessionAttribute sessionAttribute;

    @Mock
    private UserService userService;

    @Mock
    private DataSourceService dataSourceService;

    @Mock
    private McpSyncServerExchange exchange;

    private ConnectionResource connectionResource;

    @BeforeEach
    void setUp() {
        connectionResource = new ConnectionResource(sessionAttribute, userService, dataSourceService);
    }

    @Test
    void testCallWithValidConnectionId() {
        // 准备测试数据
        String connectionId = "507f1f77bcf86cd799439011";
        UserDetail mockUserDetail = mock(UserDetail.class);
        DataSourceEntity mockDataSource = new DataSourceEntity();
        mockDataSource.setId(new ObjectId(connectionId));
        mockDataSource.setName("TestConnection");
        mockDataSource.setDatabase_type("mongodb");
        mockDataSource.setStatus(DataSourceEntity.STATUS_READY);

        McpServerSession mockSession = mock(McpServerSession.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            ms.when(() -> Utils.readConnection(any())).thenReturn(createMockConnectionData(connectionId));
            ms.when(() -> Utils.toJson(any())).thenReturn("{}");

            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(dataSourceService.findAll(any(Query.class), eq(mockUserDetail)))
                    .thenReturn(Collections.singletonList(mockDataSource));

            // 执行测试
            McpSchema.ReadResourceResult result = connectionResource.call(exchange,
                    new McpSchema.ReadResourceRequest("tap://" + connectionId));

            // 验证结果
            assertNotNull(result);
            assertNotNull(result.contents());
            assertEquals(1, result.contents().size());
            verify(dataSourceService).findAll(any(Query.class), eq(mockUserDetail));
        }
    }

    @Test
    void testCallWithDataSchemaId() {
        // 准备测试数据
        String sessionId = "test-session-id";
        String connectionId = "507f1f77bcf86cd799439011";
        String dataSchemaId = "507f1f77bcf86cd799439012";
        String userId = "507f1f77bcf86cd799439013";
        UserDetail mockUserDetail = mock(UserDetail.class);
        
        DataSourceConnectionDto mockConnection = new DataSourceConnectionDto();
        Schema mockSchema = new Schema();
        Table mockTable = new Table();
        mockTable.setTableId(dataSchemaId);
        mockSchema.setTables(Collections.singletonList(mockTable));
        mockConnection.setSchema(mockSchema);

        McpServerSession mockSession = mock(McpServerSession.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            when(mockSession.getId()).thenReturn(sessionId);
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            ms.when(() -> Utils.toJson(any())).thenReturn("{}");

            when(sessionAttribute.getAttribute(sessionId, "userId")).thenReturn(userId);
            when(userService.loadUserById(new ObjectId(userId))).thenReturn(mockUserDetail);
            when(dataSourceService.getById(any(), any(), eq(false), eq(mockUserDetail)))
                    .thenReturn(mockConnection);

            // 执行测试
            McpSchema.ReadResourceResult result = connectionResource.call(exchange,
                    new McpSchema.ReadResourceRequest(String.format("tap://%s/%s", connectionId, dataSchemaId)));

            // 验证结果
            assertNotNull(result);
            assertNotNull(result.contents());
            assertEquals(1, result.contents().size());
            verify(dataSourceService).getById(any(), any(), eq(false), eq(mockUserDetail));
        }
    }

    @Test
    void testCallWithInvalidUri() {
        // 准备测试数据
        UserDetail mockUserDetail = mock(UserDetail.class);

        McpServerSession mockSession = mock(McpServerSession.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);

            // 执行测试
            McpSchema.ReadResourceResult result = connectionResource.call(exchange,
                    new McpSchema.ReadResourceRequest("invalid_uri"));

            // 验证结果
            assertNotNull(result);
            assertTrue(result.contents().isEmpty());
        }
    }

    @Test
    void testCallWithNonExistentDataSchema() {
        // 准备测试数据
        String sessionId = "test-session-id";
        String connectionId = "507f1f77bcf86cd799439011";
        String dataSchemaId = "nonobjectid";
        String userId = "507f1f77bcf86cd799439013";
        UserDetail mockUserDetail = mock(UserDetail.class);
        
        DataSourceConnectionDto mockConnection = new DataSourceConnectionDto();
        Schema mockSchema = new Schema();
        mockSchema.setTables(Collections.emptyList());
        mockConnection.setSchema(mockSchema);

        McpServerSession mockSession = mock(McpServerSession.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            when(mockSession.getId()).thenReturn(sessionId);
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            when(sessionAttribute.getAttribute(sessionId, "userId")).thenReturn(userId);
            when(userService.loadUserById(new ObjectId(userId))).thenReturn(mockUserDetail);

            // 执行测试
            McpSchema.ReadResourceResult result = connectionResource.call(exchange,
                    new McpSchema.ReadResourceRequest(String.format("tap://%s/%s", connectionId, dataSchemaId)));

            // 验证结果
            assertNotNull(result);
            assertTrue(result.contents().isEmpty());
        }
    }

    private Map<String, Object> createMockConnectionData(String connectionId) {
        Map<String, Object> data = new HashMap<>();
        data.put("id", connectionId);
        data.put("name", "TestConnection");
        data.put("databaseType", "mongodb");
        data.put("connectionType", "source");
        data.put("tableCount", 10);
        data.put("loadSchemaTime", new Date());
        data.put("tags", Arrays.asList("tag1", "tag2"));
        return data;
    }
} 