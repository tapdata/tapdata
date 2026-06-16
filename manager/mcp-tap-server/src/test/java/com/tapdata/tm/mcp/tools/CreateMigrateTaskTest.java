package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.task.service.TaskSaveService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CreateMigrateTaskTest {

    @Mock
    private SessionAttribute sessionAttribute;

    @Mock
    private UserService userService;

    @Mock
    private TaskService taskService;

    @Mock
    private TaskSaveService taskSaveService;

    @Mock
    private DataSourceService dataSourceService;

    @Mock
    private McpSyncServerExchange exchange;

    private CreateMigrateTask createMigrateTask;

    @BeforeEach
    void setUp() {
        createMigrateTask = new CreateMigrateTask(sessionAttribute, userService, taskService, taskSaveService, dataSourceService);
    }

    @Test
    void testCallCreateCustomTableReplicationTask() {
        UserDetail userDetail = mockUser();
        ObjectId sourceConnectionId = new ObjectId();
        ObjectId targetConnectionId = new ObjectId();
        DataSourceConnectionDto sourceConnection = connection(sourceConnectionId, "Source MySQL", "source", "mysql");
        DataSourceConnectionDto targetConnection = connection(targetConnectionId, "Target MongoDB", "target", "mongodb");
        TaskDto created = createdTask("ReplicateOrders");

        when(dataSourceService.findById(sourceConnectionId, userDetail)).thenReturn(sourceConnection);
        when(dataSourceService.findById(targetConnectionId, userDetail)).thenReturn(targetConnection);
        when(taskService.create(any(TaskDto.class), eq(userDetail))).thenReturn(created);

        Map<String, Object> params = validParams(sourceConnectionId, targetConnectionId);
        params.put("migrateTableSelectType", "custom");
        params.put("tableNames", List.of("orders", "customers"));

        McpSchema.CallToolResult result = createMigrateTask.call(exchange, params);

        assertNotNull(result);
        assertFalse(Boolean.TRUE.equals(result.isError()));
        assertTrue(text(result).contains("ReplicateOrders"));

        ArgumentCaptor<TaskDto> taskCaptor = ArgumentCaptor.forClass(TaskDto.class);
        verify(taskService).create(taskCaptor.capture(), eq(userDetail));
        TaskDto taskDto = taskCaptor.getValue();
        verify(taskSaveService).supplementAlarm(taskDto, userDetail);

        assertEquals("ReplicateOrders", taskDto.getName());
        assertEquals(TaskDto.SYNC_TYPE_MIGRATE, taskDto.getSyncType());
        assertEquals("initial_sync+cdc", taskDto.getType());
        assertNotNull(taskDto.getDag());
        assertEquals(2, taskDto.getDag().getNodes().size());
        assertEquals(1, taskDto.getDag().getEdges().size());
        assertEquals(1, taskDto.getSyncPoints().size());
        assertEquals(sourceConnectionId.toHexString(), taskDto.getSyncPoints().get(0).getConnectionId());
        assertEquals("current", taskDto.getSyncPoints().get(0).getPointType());

        DatabaseNode sourceNode = databaseNode(taskDto, sourceConnectionId);
        assertEquals("Source MySQL", sourceNode.getName());
        assertEquals("mysql", sourceNode.getDatabaseType());
        assertEquals("custom", sourceNode.getMigrateTableSelectType());
        assertEquals(List.of("orders", "customers"), sourceNode.getTableNames());
        assertEquals("hash-mysql", sourceNode.getAttrs().get("pdkHash"));

        DatabaseNode targetNode = databaseNode(taskDto, targetConnectionId);
        assertEquals("Target MongoDB", targetNode.getName());
        assertEquals("mongodb", targetNode.getDatabaseType());
        assertEquals("keepData", targetNode.getExistDataProcessMode());
        assertEquals("updateOrInsert", targetNode.getWriteStrategy());
    }

    @Test
    void testCallCreateExpressionReplicationTaskWithRenameNode() {
        UserDetail userDetail = mockUser();
        ObjectId sourceConnectionId = new ObjectId();
        ObjectId targetConnectionId = new ObjectId();
        DataSourceConnectionDto sourceConnection = connection(sourceConnectionId, "Source MySQL", "source", "mysql");
        DataSourceConnectionDto targetConnection = connection(targetConnectionId, "Target MongoDB", "target", "mongodb");
        TaskDto created = createdTask("ReplicateWithRename");

        when(dataSourceService.findById(sourceConnectionId, userDetail)).thenReturn(sourceConnection);
        when(dataSourceService.findById(targetConnectionId, userDetail)).thenReturn(targetConnection);
        when(taskService.create(any(TaskDto.class), eq(userDetail))).thenReturn(created);

        Map<String, Object> params = validParams(sourceConnectionId, targetConnectionId);
        params.put("taskName", "ReplicateWithRename");
        params.put("migrateTableSelectType", "expression");
        params.put("tableExpression", ".*");
        params.put("prefix", "ods_");
        params.put("suffix", "_copy");
        params.put("transferCase", "toUpperCase");
        params.put("tableRenameTableNames", List.of(Map.of(
                "originTableName", "orders",
                "currentTableName", "orders_archive"
        )));

        createMigrateTask.call(exchange, params);

        ArgumentCaptor<TaskDto> taskCaptor = ArgumentCaptor.forClass(TaskDto.class);
        verify(taskService).create(taskCaptor.capture(), eq(userDetail));
        TaskDto taskDto = taskCaptor.getValue();

        assertEquals(3, taskDto.getDag().getNodes().size());
        assertEquals(2, taskDto.getDag().getEdges().size());

        DatabaseNode sourceNode = databaseNode(taskDto, sourceConnectionId);
        assertEquals("expression", sourceNode.getMigrateTableSelectType());
        assertEquals(".*", sourceNode.getTableExpression());

        TableRenameProcessNode renameNode = taskDto.getDag().getNodes().stream()
                .filter(TableRenameProcessNode.class::isInstance)
                .map(TableRenameProcessNode.class::cast)
                .findFirst()
                .orElseThrow();
        assertEquals("Table rename", renameNode.getName());
        assertEquals("ods_", renameNode.getPrefix());
        assertEquals("_copy", renameNode.getSuffix());
        assertEquals("toUpperCase", renameNode.getTransferCase());
        assertEquals(1, renameNode.getTableNames().size());
        TableRenameTableInfo tableInfo = renameNode.getTableNames().iterator().next();
        assertEquals("orders", tableInfo.getOriginTableName());
        assertEquals("orders", tableInfo.getPreviousTableName());
        assertEquals("orders_archive", tableInfo.getCurrentTableName());

        List<Edge> edges = taskDto.getDag().getEdges();
        assertTrue(edges.stream().anyMatch(edge -> sourceNode.getId().equals(edge.getSource())
                && renameNode.getId().equals(edge.getTarget())));
        assertTrue(edges.stream().anyMatch(edge -> renameNode.getId().equals(edge.getSource())));
    }

    @Test
    void testCallCustomWithoutTableNamesThrows() {
        mockUser();
        Map<String, Object> params = validParams(new ObjectId(), new ObjectId());
        params.put("migrateTableSelectType", "custom");

        RuntimeException exception = assertThrows(RuntimeException.class, () -> createMigrateTask.call(exchange, params));

        assertTrue(exception.getMessage().contains("tableNames"));
        verify(dataSourceService, never()).findById(any(ObjectId.class), any(UserDetail.class));
        verify(taskService, never()).create(any(TaskDto.class), any(UserDetail.class));
    }

    private UserDetail mockUser() {
        UserDetail userDetail = mock(UserDetail.class);
        when(exchange.sessionId()).thenReturn("session-1");
        when(sessionAttribute.getAttribute("session-1", "userId")).thenReturn(new ObjectId().toHexString());
        when(userService.loadUserById(any(ObjectId.class))).thenReturn(userDetail);
        return userDetail;
    }

    private Map<String, Object> validParams(ObjectId sourceConnectionId, ObjectId targetConnectionId) {
        Map<String, Object> params = new HashMap<>();
        params.put("taskName", "ReplicateOrders");
        params.put("sourceConnectionId", sourceConnectionId.toHexString());
        params.put("sourceDatabaseType", "mysql");
        params.put("targetConnectionId", targetConnectionId.toHexString());
        params.put("targetDatabaseType", "mongodb");
        return params;
    }

    private DataSourceConnectionDto connection(ObjectId id, String name, String connectionType, String databaseType) {
        DataSourceConnectionDto connection = new DataSourceConnectionDto();
        connection.setId(id);
        connection.setName(name);
        connection.setConnection_type(connectionType);
        connection.setDatabase_type(databaseType);
        connection.setPdkType("pdk-" + databaseType);
        connection.setPdkHash("hash-" + databaseType);
        connection.setAccessNodeProcessId("access-" + databaseType);
        connection.setPriorityProcessId("priority-" + databaseType);
        connection.setDb_version("1.0");
        return connection;
    }

    private TaskDto createdTask(String name) {
        TaskDto taskDto = new TaskDto();
        taskDto.setId(new ObjectId());
        taskDto.setName(name);
        taskDto.setStatus(TaskDto.STATUS_EDIT);
        return taskDto;
    }

    private DatabaseNode databaseNode(TaskDto taskDto, ObjectId connectionId) {
        Node<?> node = taskDto.getDag().getNodes().stream()
                .filter(DatabaseNode.class::isInstance)
                .filter(item -> connectionId.toHexString().equals(((DatabaseNode) item).getConnectionId()))
                .findFirst()
                .orElseThrow();
        return assertInstanceOf(DatabaseNode.class, node);
    }

    private String text(McpSchema.CallToolResult result) {
        return ((McpSchema.TextContent) result.content().get(0)).text();
    }
}
