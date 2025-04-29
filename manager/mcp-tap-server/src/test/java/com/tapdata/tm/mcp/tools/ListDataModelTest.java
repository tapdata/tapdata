package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.schema.bean.Schema;
import com.tapdata.tm.commons.schema.bean.Table;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.config.security.UserDetail;
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

import java.util.ArrayList;
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
class ListDataModelTest {

    @Mock
    private SessionAttribute sessionAttribute;

    @Mock
    private DataSourceService dataSourceService;

    @Mock
    private UserService userService;

    @Mock
    private McpSyncServerExchange exchange;

    private ListDataModel listDataModel;

    @BeforeEach
    void setUp() {
        listDataModel = new ListDataModel(sessionAttribute, dataSourceService, userService);
    }

    @Test
    void testCallWithoutNameFilter() {
        // 准备测试数据
        UserDetail mockUserDetail = mock(UserDetail.class);
        DataSourceConnectionDto mockConnection = createMockConnection();

        McpServerSession mockSession = mock(McpServerSession.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(dataSourceService.getById(any(), eq(null), eq(false), eq(mockUserDetail)))
                    .thenReturn(mockConnection);

            // 执行测试
            Map<String, Object> params = new HashMap<>();
            params.put("connectionId", "507f1f77bcf86cd799439011");
            McpSchema.CallToolResult result = listDataModel.call(exchange, params);

            // 验证结果
            assertNotNull(result);
            verify(dataSourceService).getById(any(ObjectId.class), eq(null), eq(false), eq(mockUserDetail));
        }
    }

    @Test
    void testCallWithNameFilter() {
        // 准备测试数据
        UserDetail mockUserDetail = mock(UserDetail.class);
        DataSourceConnectionDto mockConnection = createMockConnection();

        McpServerSession mockSession = mock(McpServerSession.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);

            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(dataSourceService.getById(any(ObjectId.class), eq(null), eq(false), eq(mockUserDetail)))
                    .thenReturn(mockConnection);

            // 执行测试
            Map<String, Object> params = new HashMap<>();
            params.put("connectionId", "507f1f77bcf86cd799439011");
            params.put("name", "users");
            params.put("includeFields", true);
            McpSchema.CallToolResult result = listDataModel.call(exchange, params);

            // 验证结果
            assertNotNull(result);
            verify(dataSourceService).getById(any(ObjectId.class), eq(null), eq(false), eq(mockUserDetail));
        }
    }

    @Test
    void testCallWithoutConnectionId() {

        McpServerSession mockSession = mock(McpServerSession.class);
        UserDetail mockUserDetail = mock(UserDetail.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);

            // 执行测试并验证异常
            Map<String, Object> params = new HashMap<>();
            assertThrows(RuntimeException.class, () -> listDataModel.call(exchange, params));
        }
    }

    @Test
    void testCallWithInvalidSession() {
        // 执行测试并验证异常
        Map<String, Object> params = new HashMap<>();
        params.put("connectionId", "507f1f77bcf86cd799439011");
        assertThrows(RuntimeException.class, () -> listDataModel.call(exchange, params));
    }

    private DataSourceConnectionDto createMockConnection() {
        DataSourceConnectionDto connection = new DataSourceConnectionDto();
        Schema schema = new Schema();
        
        // 创建测试表
        Table table1 = new Table();
        table1.setTableId("1");
        table1.setMetaType("table");
        table1.setTableName("users");
        
        // 添加字段
        Field field1 = new Field();
        field1.setFieldName("id");
        field1.setDataType("INTEGER");
        field1.setPrimaryKey(true);
        field1.setUnique(true);

        Field field2 = new Field();
        field2.setFieldName("name");
        field2.setDataType("VARCHAR");
        
        table1.setFields(Arrays.asList(field1, field2));
        
        // 添加索引
        TableIndex index = new TableIndex();
        index.setIndexName("idx_name");
        index.setUnique(false);
        index.setColumns(new ArrayList<>());
        
        table1.setIndices(Arrays.asList(index));
        
        schema.setTables(Arrays.asList(table1));
        connection.setSchema(schema);
        
        return connection;
    }
} 