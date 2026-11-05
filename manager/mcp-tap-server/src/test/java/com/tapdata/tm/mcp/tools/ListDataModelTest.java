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
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.mcp.annotation.context.McpSyncRequestContext;

import java.util.ArrayList;
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
class ListDataModelTest {

    @Mock
    private SessionAttribute sessionAttribute;

    @Mock
    private MetadataInstancesService metadataInstancesService;

    @Mock
    private UserService userService;

    @Mock
    private McpSyncRequestContext context;

    private ListDataModel listDataModel;

    @BeforeEach
    void setUp() {
        listDataModel = new ListDataModel(new McpToolSupport(sessionAttribute, userService), metadataInstancesService);
    }

    @Test
    void testCallWithoutNameFilter() {
        // 准备测试数据
        UserDetail mockUserDetail = mock(UserDetail.class);
        Page<MetadataInstancesDto> mockPage = createMockPage();
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(metadataInstancesService.list(any(Filter.class), eq(mockUserDetail)))
                    .thenReturn(mockPage);

            // 执行测试
            List<Map<String, Object>> result = listDataModel.listDataModel(context, "507f1f77bcf86cd799439011", null, null);

            // 验证结果
            assertNotNull(result);
            assertEquals(1, result.size());
            verify(metadataInstancesService).list(any(Filter.class), eq(mockUserDetail));
        }
    }

    @Test
    void testCallWithNameFilter() {
        // 准备测试数据
        UserDetail mockUserDetail = mock(UserDetail.class);
        Page<MetadataInstancesDto> mockPage = createMockPageWithFields();
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();

            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(metadataInstancesService.list(any(Filter.class), eq(mockUserDetail)))
                    .thenReturn(mockPage);

            // 执行测试
            List<Map<String, Object>> result = listDataModel.listDataModel(context, "507f1f77bcf86cd799439011", true, "users");

            // 验证结果
            assertNotNull(result);
            assertEquals(1, result.size());
            verify(metadataInstancesService).list(any(Filter.class), eq(mockUserDetail));
        }
    }

    @Test
    void testCallWithoutConnectionId() {
        UserDetail mockUserDetail = mock(UserDetail.class);
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);

            // 执行测试并验证异常
            assertThrows(RuntimeException.class, () -> listDataModel.listDataModel(context, null, null, null));
        }
    }

    @Test
    void testCallWithInvalidSession() {
        // 执行测试并验证异常
        assertThrows(RuntimeException.class, () -> listDataModel.listDataModel(context, "507f1f77bcf86cd799439011", null, null));
    }

    @Test
    void testCallWithIncludeFieldsTrue() {
        // 准备测试数据
        UserDetail mockUserDetail = mock(UserDetail.class);
        Page<MetadataInstancesDto> mockPage = createMockPageWithFields();
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();

            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(metadataInstancesService.list(any(Filter.class), eq(mockUserDetail)))
                    .thenReturn(mockPage);

            // 执行测试
            List<Map<String, Object>> result = listDataModel.listDataModel(context, "507f1f77bcf86cd799439011", true, null);

            // 验证结果
            assertNotNull(result);
            assertEquals(1, result.size());
            assertTrue(result.get(0).containsKey("fields"));
            verify(metadataInstancesService).list(any(Filter.class), eq(mockUserDetail));
        }
    }

    @Test
    void testCallWithEmptyResult() {
        // 准备测试数据
        UserDetail mockUserDetail = mock(UserDetail.class);
        Page<MetadataInstancesDto> emptyPage = new Page<>(0, new ArrayList<>());
        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();

            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(metadataInstancesService.list(any(Filter.class), eq(mockUserDetail)))
                    .thenReturn(emptyPage);

            // 执行测试
            List<Map<String, Object>> result = listDataModel.listDataModel(context, "507f1f77bcf86cd799439011", null, null);

            // 验证结果
            assertNotNull(result);
            assertTrue(result.isEmpty());
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
