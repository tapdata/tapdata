package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.mcp.Utils;
import com.tapdata.tm.task.service.TaskSaveService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.stereotype.Component;

import java.util.*;

import static com.tapdata.tm.mcp.Utils.getStringValue;

/**
 * MCP tool for creating a replication (复制/migrate) task in TapData.
 * <p>
 * A replication task copies tables from a source database to a target database. The DAG contains a
 * source {@link DatabaseNode} and a target {@link DatabaseNode} connected by a single edge. The
 * source tables can be selected explicitly ({@code migrateTableSelectType=custom} with
 * {@code tableNames}) or by a regular expression ({@code migrateTableSelectType=expression} with
 * {@code tableExpression}, where {@code .*} matches all tables).
 */
@Slf4j
@Component
public class CreateMigrateTask extends Tool {

    private final TaskService taskService;
    private final TaskSaveService taskSaveService;
    private final DataSourceService dataSourceService;

    public CreateMigrateTask(SessionAttribute sessionAttribute,
                             UserService userService,
                             TaskService taskService,
                             TaskSaveService taskSaveService,
                             DataSourceService dataSourceService) {
        super("createMigrateTask",
                "Create a replication (复制) task in TapData that copies tables from a source database to a target database. " +
                        "Source tables are selected either explicitly (migrateTableSelectType=custom with tableNames) " +
                        "or by a regular expression (migrateTableSelectType=expression with tableExpression; use '.*' for all tables).",
                Utils.readJsonSchema("CreateMigrateTask.json"), sessionAttribute, userService);
        this.taskService = taskService;
        this.taskSaveService = taskSaveService;
        this.dataSourceService = dataSourceService;
    }

    @Override
    @SuppressWarnings("unchecked")
    public McpSchema.CallToolResult call(McpSyncServerExchange exchange, Map<String, Object> params) {
        UserDetail userDetail = getUserDetail(exchange);

        String taskName = getStringValue(params, "taskName");
        String sourceConnectionId = getStringValue(params, "sourceConnectionId");
        String sourceDatabaseType = getStringValue(params, "sourceDatabaseType");
        String targetConnectionId = getStringValue(params, "targetConnectionId");
        String targetDatabaseType = getStringValue(params, "targetDatabaseType");
        String migrateTableSelectType = getStringValue(params, "migrateTableSelectType");
        String tableExpression = getStringValue(params, "tableExpression");
        String syncType = getStringValue(params, "syncType", "initial_sync+cdc");
        List<String> tableNames = params.get("tableNames") instanceof List ? (List<String>) params.get("tableNames") : null;

        if (StringUtils.isBlank(taskName)) throw new RuntimeException("Parameter taskName is required.");
        if (StringUtils.isBlank(sourceConnectionId)) throw new RuntimeException("Parameter sourceConnectionId is required.");
        if (StringUtils.isBlank(sourceDatabaseType)) throw new RuntimeException("Parameter sourceDatabaseType is required.");
        if (StringUtils.isBlank(targetConnectionId)) throw new RuntimeException("Parameter targetConnectionId is required.");
        if (StringUtils.isBlank(targetDatabaseType)) throw new RuntimeException("Parameter targetDatabaseType is required.");
        if (StringUtils.isBlank(migrateTableSelectType)) throw new RuntimeException("Parameter migrateTableSelectType is required.");

        boolean custom = "custom".equals(migrateTableSelectType);
        boolean expression = "expression".equals(migrateTableSelectType);
        if (!custom && !expression) {
            throw new RuntimeException("Parameter migrateTableSelectType must be 'custom' or 'expression'.");
        }
        if (custom && (tableNames == null || tableNames.isEmpty())) {
            throw new RuntimeException("Parameter tableNames is required and must not be empty when migrateTableSelectType is 'custom'.");
        }
        if (expression && StringUtils.isBlank(tableExpression)) {
            throw new RuntimeException("Parameter tableExpression is required when migrateTableSelectType is 'expression' (use '.*' to replicate all tables).");
        }

        DataSourceConnectionDto sourceConnection = dataSourceService.findById(new ObjectId(sourceConnectionId), userDetail);
        DataSourceConnectionDto targetConnection = dataSourceService.findById(new ObjectId(targetConnectionId), userDetail);

        String sourceNodeId = UUID.randomUUID().toString();
        String targetNodeId = UUID.randomUUID().toString();

        DatabaseNode sourceNode = buildDatabaseNode(sourceNodeId, sourceConnectionId, sourceDatabaseType, sourceConnection);
        sourceNode.setMigrateTableSelectType(migrateTableSelectType);
        if (custom) {
            sourceNode.setTableNames(new ArrayList<>(tableNames));
        } else {
            sourceNode.setTableExpression(tableExpression);
        }

        DatabaseNode targetNode = buildDatabaseNode(targetNodeId, targetConnectionId, targetDatabaseType, targetConnection);
        targetNode.setExistDataProcessMode("keepData");
        targetNode.setWriteStrategy("updateOrInsert");

        List<Node> nodes = new ArrayList<>();
        nodes.add(sourceNode);
        nodes.add(targetNode);
        List<Edge> edges = new ArrayList<>();
        edges.add(new Edge(sourceNodeId, targetNodeId));

        Dag dagDto = new Dag(edges, nodes);
        DAG dag = DAG.build(dagDto);

        TaskDto taskDto = new TaskDto();
        taskDto.setName(taskName);
        taskDto.setDag(dag);
        taskDto.setSyncType(TaskDto.SYNC_TYPE_MIGRATE);
        taskDto.setType(syncType);

        TaskDto.SyncPoint sp = new TaskDto.SyncPoint();
        sp.setNodeId(sourceNodeId);
        sp.setNodeName(sourceNode.getName());
        sp.setConnectionId(sourceConnectionId);
        sp.setPointType("current");
        sp.setTimeZone("+8");
        sp.setIsStreamOffset(false);
        taskDto.setSyncPoints(Collections.singletonList(sp));

        taskSaveService.supplementAlarm(taskDto, userDetail);

        TaskDto result = taskService.create(taskDto, userDetail);

        Map<String, Object> response = new HashMap<>();
        response.put("id", result.getId().toHexString());
        response.put("name", result.getName());
        response.put("status", result.getStatus());
        response.put("message", "Replication task created successfully. You can view it in the TapData console.");
        return makeCallToolResult(response);
    }

    private DatabaseNode buildDatabaseNode(String nodeId, String connectionId, String databaseType, DataSourceConnectionDto connection) {
        DatabaseNode node = new DatabaseNode();
        node.setId(nodeId);
        node.setName(connection != null ? connection.getName() : connectionId);
        node.setConnectionId(connectionId);
        node.setDatabaseType(databaseType);
        node.setNodeConfig(new HashMap<>());
        node.setAttrs(buildAttrs(connection));
        return node;
    }

    private Map<String, Object> buildAttrs(DataSourceConnectionDto connection) {
        Map<String, Object> attrs = new HashMap<>();
        if (connection != null) {
            attrs.put("connectionName", connection.getName());
            attrs.put("connectionType", connection.getConnection_type());
            attrs.put("accessNodeProcessId", connection.getAccessNodeProcessId());
            attrs.put("priorityProcessId", connection.getPriorityProcessId());
            attrs.put("pdkType", connection.getPdkType());
            attrs.put("pdkHash", connection.getPdkHash());
            attrs.put("db_version", connection.getDb_version());
        }
        return attrs;
    }
}
