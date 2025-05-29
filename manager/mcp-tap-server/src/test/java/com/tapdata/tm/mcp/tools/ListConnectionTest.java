package com.tapdata.tm.mcp.tools;

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

import java.util.Arrays;
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
class ListConnectionTest {

    @Mock
    private SessionAttribute sessionAttribute;

    @Mock
    private DataSourceService dataSourceService;

    @Mock
    private UserService userService;

    @Mock
    private McpSyncServerExchange exchange;

    private ListConnection listConnection;

    @BeforeEach
    void setUp() {
        listConnection = new ListConnection(sessionAttribute, dataSourceService, userService);
    }

    @Test
    void testCallWithoutNameFilter() {
        // 准备测试数据
        UserDetail mockUserDetail = mock(UserDetail.class);
        DataSourceEntity ds1 = createMockDataSource("Connection 1", "mysql");
        DataSourceEntity ds2 = createMockDataSource("Connection 2", "postgres");

        McpServerSession mockSession = mock(McpServerSession.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(dataSourceService.findAll(any(Query.class), eq(mockUserDetail)))
                    .thenReturn(Arrays.asList(ds1, ds2));

            // 执行测试
            Map<String, Object> params = new HashMap<>();
            McpSchema.CallToolResult result = listConnection.call(exchange, params);

            // 验证结果
            assertNotNull(result);
            verify(dataSourceService).findAll(any(Query.class), eq(mockUserDetail));
        }
    }

    @Test
    void testCallWithNameFilter() {
        // 准备测试数据
        UserDetail mockUserDetail = mock(UserDetail.class);
        DataSourceEntity ds1 = createMockDataSource("Test Connection", "mysql");

        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            ms.when(() -> Utils.getSession(any())).thenReturn(mock(McpServerSession.class));
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(dataSourceService.findAll(any(Query.class), eq(mockUserDetail)))
                    .thenReturn(Arrays.asList(ds1));

            // 执行测试
            Map<String, Object> params = new HashMap<>();
            params.put("name", "Test");
            McpSchema.CallToolResult result = listConnection.call(exchange, params);

            // 验证结果
            assertNotNull(result);
            verify(dataSourceService).findAll(any(Query.class), eq(mockUserDetail));
        }
    }

    @Test
    void testCallWithInvalidSession() {
        // 执行测试并验证异常
        Map<String, Object> params = new HashMap<>();
        assertThrows(RuntimeException.class, () -> listConnection.call(exchange, params));
    }

    private DataSourceEntity createMockDataSource(String name, String type) {
        DataSourceEntity ds = new DataSourceEntity();
        ds.setId(new ObjectId());
        ds.setName(name);
        ds.setDatabase_type(type);
        ds.setStatus(DataSourceEntity.STATUS_READY);
        return ds;
    }
} 