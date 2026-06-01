package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.externalStorage.dto.ExternalStorageDto;
import com.tapdata.tm.externalStorage.service.ExternalStorageService;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.mcp.Utils;
import com.tapdata.tm.task.service.TaskSaveService;
import com.tapdata.tm.task.service.TaskService;
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

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Test cases for CreateMergeTableTask MCP tool
 *
 * @author Feynman
 * @date 2025/05/20
 */
@ExtendWith(MockitoExtension.class)
class CreateMergeTableTaskTest {

    @Mock
    private SessionAttribute sessionAttribute;

    @Mock
    private UserService userService;

    @Mock
    private TaskService taskService;

    @Mock
    private TaskSaveService taskSaveService;

    @Mock
    private ExternalStorageService externalStorageService;

    @Mock
    private DataSourceService dataSourceService;

    @Mock
    private McpSyncServerExchange exchange;

    private CreateMergeTableTask createMergeTableTask;

    @BeforeEach
    void setUp() {
        createMergeTableTask = new CreateMergeTableTask(
                sessionAttribute, userService, taskService,
                taskSaveService, externalStorageService, dataSourceService);
    }

    @Test
    void testCallSuccess() {
        UserDetail mockUserDetail = mock(UserDetail.class);
        McpServerSession mockSession = mock(McpServerSession.class);

        DataSourceConnectionDto sourceConn = createMockConnection("SourceDB", "source", "MySQL");
        DataSourceConnectionDto targetConn = createMockConnection("TargetDB", "target", "MongoDB");

        TaskDto createdTask = new TaskDto();
        createdTask.setId(new ObjectId());
        createdTask.setName("TestMergeTask");
        createdTask.setStatus("edit");

        ExternalStorageDto storageDto = new ExternalStorageDto();
        storageDto.setId(new ObjectId());

        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            ms.when(() -> Utils.getStringValue(any(), any(), any())).thenCallRealMethod();
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            ms.when(() -> Utils.makeCallToolResult(any())).thenCallRealMethod();
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(dataSourceService.findById(any(ObjectId.class), eq(mockUserDetail)))
                    .thenReturn(sourceConn).thenReturn(targetConn);
            when(externalStorageService.findOne(any())).thenReturn(storageDto);
            doNothing().when(taskSaveService).supplementAlarm(any(), any());
            when(taskService.create(any(TaskDto.class), eq(mockUserDetail))).thenReturn(createdTask);

            Map<String, Object> params = buildValidParams();
            McpSchema.CallToolResult result = createMergeTableTask.call(exchange, params);

            assertNotNull(result);
            assertFalse(result.isError() != null && result.isError());
            verify(taskService).create(any(TaskDto.class), eq(mockUserDetail));
            verify(dataSourceService, times(2)).findById(any(ObjectId.class), eq(mockUserDetail));
        }
    }

    @Test
    void testCallWithoutTaskName() {
        UserDetail mockUserDetail = mock(UserDetail.class);
        McpServerSession mockSession = mock(McpServerSession.class);

        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            ms.when(() -> Utils.getSession(any())).thenReturn(mockSession);
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);

            Map<String, Object> params = new HashMap<>();
            RuntimeException exception = assertThrows(RuntimeException.class,
                    () -> createMergeTableTask.call(exchange, params));
            assertTrue(exception.getMessage().contains("taskName"));
        }
    }

    @Test
    void testCallWithInvalidSession() {
        Map<String, Object> params = buildValidParams();
        assertThrows(RuntimeException.class, () -> createMergeTableTask.call(exchange, params));
    }

    private Map<String, Object> buildValidParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("taskName", "TestMergeTask");
        params.put("sourceConnectionId", new ObjectId().toHexString());
        params.put("sourceDatabaseType", "MySQL");
        params.put("targetConnectionId", new ObjectId().toHexString());
        params.put("targetDatabaseType", "MongoDB");
        params.put("targetTableName", "merged_output");

        Map<String, Object> mainTable = new HashMap<>();
        mainTable.put("tableName", "orders");
        mainTable.put("arrayKeys", Arrays.asList("id"));
        params.put("mainTable", mainTable);

        Map<String, Object> childTable = new HashMap<>();
        childTable.put("tableName", "order_items");
        childTable.put("targetPath", "items");
        childTable.put("joinKeys", Arrays.asList(Map.of("source", "order_id", "target", "id")));
        params.put("childTables", Arrays.asList(childTable));

        return params;
    }

    private DataSourceConnectionDto createMockConnection(String name, String type, String dbType) {
        DataSourceConnectionDto conn = new DataSourceConnectionDto();
        conn.setId(new ObjectId());
        conn.setName(name);
        conn.setConnection_type(type);
        conn.setDatabase_type(dbType);
        conn.setPdkType("pdk");
        conn.setPdkHash("hash123");
        conn.setAccessNodeProcessId("node1");
        conn.setDb_version("8.0");
        return conn;
    }
}
