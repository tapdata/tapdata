package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.mcp.Utils;
import com.tapdata.tm.user.service.UserService;
import org.bson.types.ObjectId;
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
    private McpSyncRequestContext context;

    private ListConnection listConnection;

    @BeforeEach
    void setUp() {
        listConnection = new ListConnection(new McpToolSupport(sessionAttribute, userService), dataSourceService);
    }

    @Test
    void testCallWithoutNameFilter() {
        // 准备测试数据
        UserDetail mockUserDetail = mock(UserDetail.class);
        DataSourceEntity ds1 = createMockDataSource("Connection 1", "mysql");
        DataSourceEntity ds2 = createMockDataSource("Connection 2", "postgres");
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(dataSourceService.findAll(any(Query.class), eq(mockUserDetail)))
                    .thenReturn(Arrays.asList(ds1, ds2));

            // 执行测试
            List<Map<String, Object>> result = listConnection.listConnection(context, null);

            // 验证结果
            assertNotNull(result);
            assertEquals(2, result.size());
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
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(dataSourceService.findAll(any(Query.class), eq(mockUserDetail)))
                    .thenReturn(Arrays.asList(ds1));

            // 执行测试
            List<Map<String, Object>> result = listConnection.listConnection(context, "Test");

            // 验证结果
            assertNotNull(result);
            assertEquals(1, result.size());
            verify(dataSourceService).findAll(any(Query.class), eq(mockUserDetail));
        }
    }

    @Test
    void testCallWithInvalidSession() {
        // 执行测试并验证异常
        assertThrows(RuntimeException.class, () -> listConnection.listConnection(context, null));
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
