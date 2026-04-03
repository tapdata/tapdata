package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.externalStorage.service.ExternalStorageService;
import com.tapdata.tm.mcp.SessionAttribute;
import com.tapdata.tm.task.service.TaskSaveService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.*;

import static com.tapdata.tm.mcp.Utils.*;

/**
 * MCP tool for creating a master-slave merge (主从合并) task.
 * This tool creates a task with a DAG containing:
 * - Source table nodes (main table + child tables)
 * - A merge_table_processor node for joining
 * - A target table node for the merged output
 */
@Slf4j
@Component
public class CreateMergeTableTask extends Tool {

    private final TaskService taskService;
    private final TaskSaveService taskSaveService;
    private final ExternalStorageService externalStorageService;

    public CreateMergeTableTask(SessionAttribute sessionAttribute,
                                UserService userService,
                                TaskService taskService,
                                TaskSaveService taskSaveService,
                                ExternalStorageService externalStorageService) {
        super("createMergeTableTask",
                "Create a master-slave merge (主从合并) task in TapData. " +
                "This task merges multiple source tables into one target document/table by joining child tables into the main table. " +
                "Typically used to build wide tables or nested documents (e.g., embedding child records into a parent document in MongoDB).",
                readJsonSchema("CreateMergeTableTask.json"), sessionAttribute, userService);
        this.taskService = taskService;
        this.taskSaveService = taskSaveService;
        this.externalStorageService = externalStorageService;
    }

    @Override
    public McpSchema.CallToolResult call(McpSyncServerExchange exchange, Map<String, Object> params) {
        UserDetail userDetail = getUserDetail(exchange);

        // Extract parameters
        String taskName = getStringValue(params, "taskName");
        String sourceConnectionId = getStringValue(params, "sourceConnectionId");
        String sourceDatabaseType = getStringValue(params, "sourceDatabaseType");
        String targetConnectionId = getStringValue(params, "targetConnectionId");
        String targetDatabaseType = getStringValue(params, "targetDatabaseType");
        String targetTableName = getStringValue(params, "targetTableName");
        String syncType = getStringValue(params, "syncType", "initial_sync+cdc");

        Map<String, Object> mainTableParam = (Map<String, Object>) params.get("mainTable");
        List<Map<String, Object>> childTablesParam = (List<Map<String, Object>>) params.get("childTables");

        // Validate required parameters
        if (taskName == null) throw new RuntimeException("Parameter taskName is required.");
        if (sourceConnectionId == null) throw new RuntimeException("Parameter sourceConnectionId is required.");
        if (sourceDatabaseType == null) throw new RuntimeException("Parameter sourceDatabaseType is required.");
        if (targetConnectionId == null) throw new RuntimeException("Parameter targetConnectionId is required.");
        if (targetDatabaseType == null) throw new RuntimeException("Parameter targetDatabaseType is required.");
        if (targetTableName == null) throw new RuntimeException("Parameter targetTableName is required.");
        if (mainTableParam == null) throw new RuntimeException("Parameter mainTable is required.");
        if (childTablesParam == null || childTablesParam.isEmpty())
            throw new RuntimeException("Parameter childTables is required and must not be empty.");

        String mainTableName = (String) mainTableParam.get("tableName");
        List<String> mainArrayKeys = (List<String>) mainTableParam.get("arrayKeys");

        // Build source nodes, merge node, target node, and edges
        List<Node> nodes = new ArrayList<>();
        List<Edge> edges = new ArrayList<>();

        // 1. Main source table node
        String mainNodeId = UUID.randomUUID().toString();
        TableNode mainSourceNode = buildSourceTableNode(mainNodeId, mainTableName, sourceConnectionId, sourceDatabaseType);
        nodes.add(mainSourceNode);

        // 2. Child source table nodes
        List<MergeTableProperties> childProperties = new ArrayList<>();
        for (Map<String, Object> childParam : childTablesParam) {
            String childNodeId = UUID.randomUUID().toString();
            String childTableName = (String) childParam.get("tableName");

            TableNode childSourceNode = buildSourceTableNode(childNodeId, childTableName, sourceConnectionId, sourceDatabaseType);
            nodes.add(childSourceNode);

            // Build child merge properties
            MergeTableProperties childProp = new MergeTableProperties();
            String mergeTypeStr = childParam.containsKey("mergeType") ? (String) childParam.get("mergeType") : "updateWrite";
            childProp.setMergeType(MergeTableProperties.MergeType.valueOf(mergeTypeStr));
            childProp.setTableName(childTableName);
            childProp.setId(childNodeId);
            childProp.setTargetPath((String) childParam.get("targetPath"));
            childProp.setChildren(new ArrayList<>());
            childProp.setEnableUpdateJoinKeyValue(false);

            // Join keys
            List<Map<String, Object>> joinKeysParam = (List<Map<String, Object>>) childParam.get("joinKeys");
            List<Map<String, String>> joinKeys = new ArrayList<>();
            for (Map<String, Object> jk : joinKeysParam) {
                Map<String, String> joinKey = new HashMap<>();
                joinKey.put("source", (String) jk.get("source"));
                joinKey.put("target", (String) jk.get("target"));
                joinKeys.add(joinKey);
            }
            childProp.setJoinKeys(joinKeys);

            // Array keys
            List<String> childArrayKeys = (List<String>) childParam.get("arrayKeys");
            if (childArrayKeys != null) {
                childProp.setArrayKeys(childArrayKeys);
            } else {
                // Default: use the source fields from joinKeys
                List<String> defaultArrayKeys = new ArrayList<>();
                for (Map<String, String> jk : joinKeys) {
                    defaultArrayKeys.add(jk.get("source"));
                }
                childProp.setArrayKeys(defaultArrayKeys);
            }

            childProperties.add(childProp);

            // Edge: child source -> merge node (will add after merge node is created)
        }

        // 3. Build main merge property
        MergeTableProperties mainProp = new MergeTableProperties();
        mainProp.setMergeType(MergeTableProperties.MergeType.updateOrInsert);
        mainProp.setTableName(mainTableName);
        mainProp.setId(mainNodeId);
        mainProp.setArrayKeys(mainArrayKeys);
        mainProp.setChildren(childProperties);
        mainProp.setEnableUpdateJoinKeyValue(false);

        // 4. Merge table processor node
        String mergeNodeId = UUID.randomUUID().toString();
        MergeTableNode mergeNode = new MergeTableNode();
        mergeNode.setId(mergeNodeId);
        mergeNode.setName("主从合并");
        mergeNode.setMergeProperties(Collections.singletonList(mainProp));
        mergeNode.setMergeMode(MergeTableNode.MAIN_TABLE_FIRST_MERGE_MODE);
        // Set default external storage id
        String externalStorageId = getDefaultExternalStorageId();
        mergeNode.setExternalStorageId(externalStorageId);
        nodes.add(mergeNode);

        // 5. Target table node
        String targetNodeId = UUID.randomUUID().toString();
        TableNode targetNode = buildTargetTableNode(targetNodeId, targetTableName, targetConnectionId, targetDatabaseType, mainArrayKeys);
        nodes.add(targetNode);

        // 6. Build edges: each source -> merge, merge -> target
        edges.add(new Edge(mainNodeId, mergeNodeId));
        for (MergeTableProperties childProp : childProperties) {
            edges.add(new Edge(childProp.getId(), mergeNodeId));
        }
        edges.add(new Edge(mergeNodeId, targetNodeId));

        // 7. Build DAG
        Dag dagDto = new Dag(edges, nodes);
        DAG dag = DAG.build(dagDto);

        // 8. Build TaskDto
        TaskDto taskDto = new TaskDto();
        taskDto.setName(taskName);
        taskDto.setDag(dag);
        taskDto.setSyncType(TaskDto.SYNC_TYPE_SYNC);
        taskDto.setType(syncType);

        // Build sync points for source nodes
        List<TaskDto.SyncPoint> syncPoints = new ArrayList<>();
        for (Node<?> node : nodes) {
            if (node instanceof TableNode tableNode && node.isDataNode()) {
                if (tableNode.getConnectionId() != null && tableNode.getConnectionId().equals(sourceConnectionId)) {
                    TaskDto.SyncPoint sp = new TaskDto.SyncPoint();
                    sp.setNodeId(node.getId());
                    sp.setNodeName(node.getName());
                    sp.setConnectionId(tableNode.getConnectionId());
                    sp.setPointType("current");
                    sp.setTimeZone("+8");
                    sp.setIsStreamOffset(false);
                    syncPoints.add(sp);
                }
            }
        }
        taskDto.setSyncPoints(syncPoints);

        // Supplement alarm settings
        taskSaveService.supplementAlarm(taskDto, userDetail);

        // Create task
        TaskDto result = taskService.create(taskDto, userDetail);

        // Build response
        Map<String, Object> response = new HashMap<>();
        response.put("id", result.getId().toHexString());
        response.put("name", result.getName());
        response.put("status", result.getStatus());
        response.put("message", "Task created successfully. You can view it in the TapData console.");

        return makeCallToolResult(response);
    }

    private TableNode buildSourceTableNode(String nodeId, String tableName, String connectionId, String databaseType) {
        TableNode node = new TableNode();
        node.setId(nodeId);
        node.setName(tableName);
        node.setTableName(tableName);
        node.setConnectionId(connectionId);
        node.setDatabaseType(databaseType);
        node.setReadBatchSize(100);
        node.setIncreaseReadSize(1);
        node.setWriteStrategy("updateOrInsert");
        node.setSyncForeignKeyEnable(true);
        node.setNoPkSyncMode("ADD_HASH");
        node.setExistDataProcessMode("keepData");
        return node;
    }

    private TableNode buildTargetTableNode(String nodeId, String tableName, String connectionId, String databaseType, List<String> updateConditionFields) {
        TableNode node = new TableNode();
        node.setId(nodeId);
        node.setName(tableName);
        node.setTableName(tableName);
        node.setConnectionId(connectionId);
        node.setDatabaseType(databaseType);
        node.setUpdateConditionFields(updateConditionFields);
        node.setExistDataProcessMode("keepData");
        node.setWriteStrategy("updateOrInsert");
        node.setSyncForeignKeyEnable(true);
        node.setUniqueIndexEnable(true);
        node.setNoPkSyncMode("ADD_HASH");
        return node;
    }

    private String getDefaultExternalStorageId() {
        try {
            var dto = externalStorageService.findOne(Query.query(Criteria.where("defaultStorage").is(true)));
            if (dto != null && dto.getId() != null) {
                return dto.getId().toHexString();
            }
        } catch (Exception e) {
            log.warn("Failed to get default external storage", e);
        }
        return null;
    }
}

