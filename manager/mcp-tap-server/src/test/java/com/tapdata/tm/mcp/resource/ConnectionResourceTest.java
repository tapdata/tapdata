package com.tapdata.tm.mcp.resource;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.bean.Schema;
import com.tapdata.tm.commons.schema.bean.Table;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.entity.DataSourceEntity;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.mcp.Utils;
import com.tapdata.tm.mcp.tools.McpToolSupport;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.mcp.annotation.context.McpSyncRequestContext;
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
    private McpToolSupport toolSupport;

    @Mock
    private DataSourceService dataSourceService;

    @Mock
    private McpSyncRequestContext context;

    private ConnectionResource connectionResource;

    @BeforeEach
    void setUp() {
        connectionResource = new ConnectionResource(toolSupport, dataSourceService);
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

        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.readConnection(any())).thenReturn(createMockConnectionData(connectionId));
            ms.when(() -> Utils.toJson(any())).thenReturn("{}");

            when(toolSupport.getUserDetail(context)).thenReturn(mockUserDetail);
            when(dataSourceService.findAll(any(Query.class), eq(mockUserDetail)))
                    .thenReturn(Collections.singletonList(mockDataSource));

            // 执行测试
            String result = connectionResource.getConnection(context, connectionId);

            // 验证结果
            assertEquals("{}", result);
            verify(dataSourceService).findAll(any(Query.class), eq(mockUserDetail));
        }
    }

    @Test
    void testCallWithDataSchemaId() {
        // 准备测试数据
        String connectionId = "507f1f77bcf86cd799439011";
        String dataSchemaId = "507f1f77bcf86cd799439012";
        UserDetail mockUserDetail = mock(UserDetail.class);
        
        DataSourceConnectionDto mockConnection = new DataSourceConnectionDto();
        Schema mockSchema = new Schema();
        Table mockTable = new Table();
        mockTable.setTableId(dataSchemaId);
        mockSchema.setTables(Collections.singletonList(mockTable));
        mockConnection.setSchema(mockSchema);

        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            // 设置 mock 行为
            ms.when(() -> Utils.toJson(any())).thenReturn("{}");

            when(toolSupport.getUserDetail(context)).thenReturn(mockUserDetail);
            when(dataSourceService.getById(any(), any(), eq(false), eq(mockUserDetail)))
                    .thenReturn(mockConnection);

            // 执行测试
            String result = connectionResource.getDataModel(context, connectionId, dataSchemaId);

            // 验证结果
            assertEquals("{}", result);
            verify(dataSourceService).getById(any(), any(), eq(false), eq(mockUserDetail));
        }
    }

    @Test
    void testCallWithInvalidUri() {
        String result = connectionResource.read(context, "invalid_uri");

        assertNull(result);
        verifyNoInteractions(toolSupport, dataSourceService);
    }

    @Test
    void testCallWithNonExistentDataSchema() {
        // 准备测试数据
        String connectionId = "507f1f77bcf86cd799439011";
        String dataSchemaId = "507f1f77bcf86cd799439014";
        UserDetail mockUserDetail = mock(UserDetail.class);
        
        DataSourceConnectionDto mockConnection = new DataSourceConnectionDto();
        Schema mockSchema = new Schema();
        mockSchema.setTables(Collections.emptyList());
        mockConnection.setSchema(mockSchema);

        when(toolSupport.getUserDetail(context)).thenReturn(mockUserDetail);
        when(dataSourceService.getById(any(), any(), eq(false), eq(mockUserDetail)))
                .thenReturn(mockConnection);

        // 执行测试
        String result = connectionResource.getDataModel(context, connectionId, dataSchemaId);

        // 验证结果
        assertNull(result);
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
