package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.externalStorage.service.ExternalStorageService;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.mcp.Utils;
import com.tapdata.tm.task.service.TaskSaveService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.mcp.annotation.context.McpSyncRequestContext;

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
    private McpSyncRequestContext context;

    private CreateMergeTableTask createMergeTableTask;

    @BeforeEach
    void setUp() {
        createMergeTableTask = new CreateMergeTableTask(
                new McpToolSupport(sessionAttribute, userService), taskService,
                taskSaveService, externalStorageService, dataSourceService);
    }

    @Test
    void testCallSuccess() {
        UserDetail mockUserDetail = mock(UserDetail.class);

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
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);
            when(dataSourceService.findById(any(ObjectId.class), eq(mockUserDetail)))
                    .thenReturn(sourceConn).thenReturn(targetConn);
            when(externalStorageService.findOne(any())).thenReturn(storageDto);
            doNothing().when(taskSaveService).supplementAlarm(any(), any());
            when(taskService.create(any(TaskDto.class), eq(mockUserDetail))).thenReturn(createdTask);

            TestMergeInput input = buildValidInput();
            Map<String, Object> result = createMergeTableTask.createMergeTableTask(context,
                    input.taskName,
                    input.sourceConnectionId,
                    input.sourceDatabaseType,
                    input.targetConnectionId,
                    input.targetDatabaseType,
                    input.targetTableName,
                    input.mainTable,
                    input.childTables,
                    null);

            assertNotNull(result);
            assertEquals("TestMergeTask", result.get("name"));
            verify(taskService).create(any(TaskDto.class), eq(mockUserDetail));
            verify(dataSourceService, times(2)).findById(any(ObjectId.class), eq(mockUserDetail));
        }
    }

    @Test
    void testCallWithoutTaskName() {
        UserDetail mockUserDetail = mock(UserDetail.class);

        try (MockedStatic<Utils> ms = mockStatic(Utils.class)) {
            ms.when(() -> Utils.getStringValue(any(), any())).thenCallRealMethod();
            when(sessionAttribute.getAttribute(any(), eq("userId"))).thenReturn("123");
            when(userService.loadUserById(any())).thenReturn(mockUserDetail);

            RuntimeException exception = assertThrows(RuntimeException.class,
                    () -> createMergeTableTask.createMergeTableTask(context,
                            null, null, null, null, null, null, null, null, null));
            assertTrue(exception.getMessage().contains("taskName"));
        }
    }

    @Test
    void testCallWithInvalidSession() {
        TestMergeInput input = buildValidInput();
        assertThrows(RuntimeException.class, () -> createMergeTableTask.createMergeTableTask(context,
                input.taskName,
                input.sourceConnectionId,
                input.sourceDatabaseType,
                input.targetConnectionId,
                input.targetDatabaseType,
                input.targetTableName,
                input.mainTable,
                input.childTables,
                null));
    }

    private TestMergeInput buildValidInput() {
        TestMergeInput input = new TestMergeInput();
        input.taskName = "TestMergeTask";
        input.sourceConnectionId = new ObjectId().toHexString();
        input.sourceDatabaseType = "MySQL";
        input.targetConnectionId = new ObjectId().toHexString();
        input.targetDatabaseType = "MongoDB";
        input.targetTableName = "merged_output";

        CreateMergeTableTask.MainTable mainTable = new CreateMergeTableTask.MainTable();
        mainTable.tableName = "orders";
        mainTable.arrayKeys = Arrays.asList("id");
        input.mainTable = mainTable;

        CreateMergeTableTask.JoinKey joinKey = new CreateMergeTableTask.JoinKey();
        joinKey.source = "order_id";
        joinKey.target = "id";

        CreateMergeTableTask.ChildTable childTable = new CreateMergeTableTask.ChildTable();
        childTable.tableName = "order_items";
        childTable.targetPath = "items";
        childTable.joinKeys = Arrays.asList(joinKey);
        input.childTables = Arrays.asList(childTable);

        return input;
    }

    private static class TestMergeInput {
        String taskName;
        String sourceConnectionId;
        String sourceDatabaseType;
        String targetConnectionId;
        String targetDatabaseType;
        String targetTableName;
        CreateMergeTableTask.MainTable mainTable;
        List<CreateMergeTableTask.ChildTable> childTables;
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
