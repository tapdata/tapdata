package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.TableIndex;
import com.tapdata.tm.commons.schema.TableIndexColumn;
import com.tapdata.tm.commons.schema.Field;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.mcp.Utils;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
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
class ListDataModelTest {

    @Mock
    private SessionAttribute sessionAttribute;

    @Mock
    private MetadataInstancesService metadataInstancesService;

    @Mock
    private UserService userService;

    @Mock
    private McpSyncServerExchange exchange;

    private ListDataModel listDataModel;

    @BeforeEach
    void setUp() {
        listDataModel = new ListDataModel(sessionAttribute, metadataInstancesService, userService);
    }

    @Test
    void testCallWithoutNameFilter() {
        // 准备测试数据
        UserDetail mockUserDetail = mock(UserDetail.class);
        Page<MetadataInstancesDto> mockPage = createMockPage();

        McpServerSession mockSession = mock(McpServerSession.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            ms.when(() -> Utils.makeCallToolResult(any())).thenCallRealMethod();
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(metadataInstancesService.list(any(Filter.class), eq(mockUserDetail)))
                    .thenReturn(mockPage);

            // 执行测试
            Map<String, Object> params = new HashMap<>();
            params.put("connectionId", "507f1f77bcf86cd799439011");
            McpSchema.CallToolResult result = listDataModel.call(exchange, params);

            // 验证结果
            assertNotNull(result);
            verify(metadataInstancesService).list(any(Filter.class), eq(mockUserDetail));
        }
    }

    @Test
    void testCallWithNameFilter() {
        // 准备测试数据
        UserDetail mockUserDetail = mock(UserDetail.class);
        Page<MetadataInstancesDto> mockPage = createMockPageWithFields();

        McpServerSession mockSession = mock(McpServerSession.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            ms.when(() -> Utils.makeCallToolResult(any())).thenCallRealMethod();

            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(metadataInstancesService.list(any(Filter.class), eq(mockUserDetail)))
                    .thenReturn(mockPage);

            // 执行测试
            Map<String, Object> params = new HashMap<>();
            params.put("connectionId", "507f1f77bcf86cd799439011");
            params.put("name", "users");
            params.put("includeFields", true);
            McpSchema.CallToolResult result = listDataModel.call(exchange, params);

            // 验证结果
            assertNotNull(result);
            verify(metadataInstancesService).list(any(Filter.class), eq(mockUserDetail));
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

    @Test
    void testCallWithIncludeFieldsTrue() {
        // 准备测试数据
        UserDetail mockUserDetail = mock(UserDetail.class);
        Page<MetadataInstancesDto> mockPage = createMockPageWithFields();

        McpServerSession mockSession = mock(McpServerSession.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            ms.when(() -> Utils.makeCallToolResult(any())).thenCallRealMethod();

            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(metadataInstancesService.list(any(Filter.class), eq(mockUserDetail)))
                    .thenReturn(mockPage);

            // 执行测试
            Map<String, Object> params = new HashMap<>();
            params.put("connectionId", "507f1f77bcf86cd799439011");
            params.put("includeFields", true);
            McpSchema.CallToolResult result = listDataModel.call(exchange, params);

            // 验证结果
            assertNotNull(result);
            assertFalse(result.isError() != null && result.isError());
            verify(metadataInstancesService).list(any(Filter.class), eq(mockUserDetail));
        }
    }

    @Test
    void testCallWithEmptyResult() {
        // 准备测试数据
        UserDetail mockUserDetail = mock(UserDetail.class);
        Page<MetadataInstancesDto> emptyPage = new Page<>(0, new ArrayList<>());

        McpServerSession mockSession = mock(McpServerSession.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            ms.when(() -> Utils.makeCallToolResult(any())).thenCallRealMethod();

            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(metadataInstancesService.list(any(Filter.class), eq(mockUserDetail)))
                    .thenReturn(emptyPage);

            // 执行测试
            Map<String, Object> params = new HashMap<>();
            params.put("connectionId", "507f1f77bcf86cd799439011");
            McpSchema.CallToolResult result = listDataModel.call(exchange, params);

            // 验证结果
            assertNotNull(result);
            verify(metadataInstancesService).list(any(Filter.class), eq(mockUserDetail));
        }
    }

    private Page<MetadataInstancesDto> createMockPage() {
        List<MetadataInstancesDto> items = new ArrayList<>();

        MetadataInstancesDto metadata = new MetadataInstancesDto();
        metadata.setId(new ObjectId());
        metadata.setOriginalName("users");
        metadata.setMetaType("table");

        items.add(metadata);

        return new Page<>(1, items);
    }

    private Page<MetadataInstancesDto> createMockPageWithFields() {
        List<MetadataInstancesDto> items = new ArrayList<>();

        MetadataInstancesDto metadata = new MetadataInstancesDto();
        metadata.setId(new ObjectId());
        metadata.setOriginalName("users");
        metadata.setMetaType("table");

        // 添加字段
        Field field1 = new Field();
        field1.setFieldName("id");
        field1.setDataType("INTEGER");
        field1.setPrimaryKey(true);
        field1.setUnique(true);

        Field field2 = new Field();
        field2.setFieldName("name");
        field2.setDataType("VARCHAR");

        metadata.setFields(Arrays.asList(field1, field2));

        // 添加索引
        TableIndex index = new TableIndex();
        index.setIndexName("idx_name");
        index.setUnique(false);
        TableIndexColumn column = new TableIndexColumn();
        column.setColumnName("name");
        index.setColumns(Arrays.asList(column));

        metadata.setIndices(Arrays.asList(index));

        items.add(metadata);

        return new Page<>(1, items);
    }
}