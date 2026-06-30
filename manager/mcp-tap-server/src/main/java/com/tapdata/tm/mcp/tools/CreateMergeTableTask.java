package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.MergeTableProperties;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.externalStorage.service.ExternalStorageService;
import com.tapdata.tm.task.service.TaskSaveService;
import com.tapdata.tm.task.service.TaskService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.ai.mcp.annotation.context.McpSyncRequestContext;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * MCP tool for creating a master-slave merge (主从合并) task.
 * This tool creates a task with a DAG containing:
 * - Source table nodes (main table + child tables)
 * - A merge_table_processor node for joining
 * - A target table node for the merged output
 */
@Slf4j
@Component
public class CreateMergeTableTask {

    private final McpToolSupport toolSupport;
    private final TaskService taskService;
    private final TaskSaveService taskSaveService;
    private final ExternalStorageService externalStorageService;
    private final DataSourceService dataSourceService;

    public CreateMergeTableTask(McpToolSupport toolSupport,
                                TaskService taskService,
                                TaskSaveService taskSaveService,
                                ExternalStorageService externalStorageService,
                                DataSourceService dataSourceService) {
        this.toolSupport = toolSupport;
        this.taskService = taskService;
        this.taskSaveService = taskSaveService;
        this.externalStorageService = externalStorageService;
        this.dataSourceService = dataSourceService;
    }

    @McpTool(name = "createMergeTableTask", description = "Create a master-slave merge task in TapData that joins child source tables into one target table or document.")
    public Map<String, Object> createMergeTableTask(
            McpSyncRequestContext context,
            @McpToolParam(description = "Task name.") String taskName,
            @McpToolParam(description = "Source connection id.") String sourceConnectionId,
            @McpToolParam(description = "Source database type, for example mongodb, mysql, oracle, sqlserver, or postgresql.") String sourceDatabaseType,
            @McpToolParam(description = "Target connection id.") String targetConnectionId,
            @McpToolParam(description = "Target database type, for example mongodb, mysql, oracle, sqlserver, or postgresql.") String targetDatabaseType,
            @McpToolParam(description = "Target table or collection name.") String targetTableName,
            @McpToolParam(description = "Main source table definition.") MainTable mainTable,
            @McpToolParam(description = "Child source table definitions and join rules.") List<ChildTable> childTables,
            @McpToolParam(required = false, description = "Task sync type. Defaults to initial_sync+cdc.") String syncType) {
        UserDetail userDetail = toolSupport.getUserDetail(context);
        String resolvedSyncType = StringUtils.defaultIfBlank(syncType, "initial_sync+cdc");

        // Validate required parameters
        if (StringUtils.isBlank(taskName)) throw new RuntimeException("Parameter taskName is required.");
        if (StringUtils.isBlank(sourceConnectionId)) throw new RuntimeException("Parameter sourceConnectionId is required.");
        if (StringUtils.isBlank(sourceDatabaseType)) throw new RuntimeException("Parameter sourceDatabaseType is required.");
        if (StringUtils.isBlank(targetConnectionId)) throw new RuntimeException("Parameter targetConnectionId is required.");
        if (StringUtils.isBlank(targetDatabaseType)) throw new RuntimeException("Parameter targetDatabaseType is required.");
        if (StringUtils.isBlank(targetTableName)) throw new RuntimeException("Parameter targetTableName is required.");
        if (mainTable == null) throw new RuntimeException("Parameter mainTable is required.");
        if (StringUtils.isBlank(mainTable.tableName)) throw new RuntimeException("Parameter mainTable.tableName is required.");
        if (childTables == null || childTables.isEmpty())
            throw new RuntimeException("Parameter childTables is required and must not be empty.");

        String mainTableName = mainTable.tableName;
        List<String> mainArrayKeys = mainTable.arrayKeys;

        // Fetch connection info for attrs
        DataSourceConnectionDto sourceConnection = dataSourceService.findById(new ObjectId(sourceConnectionId), userDetail);
        DataSourceConnectionDto targetConnection = dataSourceService.findById(new ObjectId(targetConnectionId), userDetail);

        // Build source nodes, merge node, target node, and edges
        List<Node> nodes = new ArrayList<>();
        List<Edge> edges = new ArrayList<>();

        // 1. Main source table node
        String mainNodeId = UUID.randomUUID().toString();
        TableNode mainSourceNode = buildSourceTableNode(mainNodeId, mainTableName, sourceConnectionId, sourceDatabaseType, sourceConnection);
        nodes.add(mainSourceNode);

        // 2. Child source table nodes
        List<MergeTableProperties> childProperties = new ArrayList<>();
        for (ChildTable childTable : childTables) {
            if (childTable == null) {
                throw new RuntimeException("Parameter childTables must contain objects.");
            }
            String childNodeId = UUID.randomUUID().toString();
            String childTableName = childTable.tableName;
            if (StringUtils.isBlank(childTableName)) {
                throw new RuntimeException("Parameter childTables.tableName is required.");
            }

            TableNode childSourceNode = buildSourceTableNode(childNodeId, childTableName, sourceConnectionId, sourceDatabaseType, sourceConnection);
            nodes.add(childSourceNode);

            // Build child merge properties
            MergeTableProperties childProp = new MergeTableProperties();
            String mergeTypeStr = StringUtils.defaultIfBlank(childTable.mergeType, "updateWrite");
            childProp.setMergeType(MergeTableProperties.MergeType.valueOf(mergeTypeStr));
            childProp.setTableName(childTableName);
            childProp.setId(childNodeId);
            childProp.setTargetPath(childTable.targetPath);
            childProp.setChildren(new ArrayList<>());
            childProp.setEnableUpdateJoinKeyValue(false);

            // Join keys
            List<JoinKey> joinKeysParam = childTable.joinKeys;
            if (joinKeysParam == null || joinKeysParam.isEmpty()) {
                throw new RuntimeException("Parameter childTables.joinKeys is required and must not be empty.");
            }
            List<Map<String, String>> joinKeys = new ArrayList<>();
            for (JoinKey jk : joinKeysParam) {
                Map<String, String> joinKey = new HashMap<>();
                joinKey.put("source", jk.source);
                joinKey.put("target", jk.target);
                joinKeys.add(joinKey);
            }
            childProp.setJoinKeys(joinKeys);

            // Array keys
            List<String> childArrayKeys = childTable.arrayKeys;
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
        mergeNode.setName("Merge Table");
        mergeNode.setMergeProperties(Collections.singletonList(mainProp));
        mergeNode.setMergeMode(MergeTableNode.MAIN_TABLE_FIRST_MERGE_MODE);
        // Set default external storage id
        String externalStorageId = getDefaultExternalStorageId();
        mergeNode.setExternalStorageId(externalStorageId);
        nodes.add(mergeNode);

        // 5. Target table node
        String targetNodeId = UUID.randomUUID().toString();
        TableNode targetNode = buildTargetTableNode(targetNodeId, targetTableName, targetConnectionId, targetDatabaseType, mainArrayKeys, targetConnection);
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
        taskDto.setType(resolvedSyncType);

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

        return response;
    }

    private TableNode buildSourceTableNode(String nodeId, String tableName, String connectionId, String databaseType, DataSourceConnectionDto connection) {
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
        node.setAttrs(buildAttrs(connection));
        return node;
    }

    private TableNode buildTargetTableNode(String nodeId, String tableName, String connectionId, String databaseType, List<String> updateConditionFields, DataSourceConnectionDto connection) {
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

    public static class MainTable {
        @McpToolParam(description = "Main source table name.")
        public String tableName;

        @McpToolParam(required = false, description = "Fields used as update condition keys for the main table.")
        public List<String> arrayKeys;
    }

    public static class ChildTable {
        @McpToolParam(description = "Child source table name.")
        public String tableName;

        @McpToolParam(description = "Target field path where child rows are merged into the main record.")
        public String targetPath;

        @McpToolParam(description = "Join keys between the child table and the main table.")
        public List<JoinKey> joinKeys;

        @McpToolParam(required = false, description = "Merge strategy. Defaults to updateWrite.")
        public String mergeType;

        @McpToolParam(required = false, description = "Child source fields used as array keys. Defaults to joinKeys.source.")
        public List<String> arrayKeys;
    }

    public static class JoinKey {
        @McpToolParam(description = "Child table join field.")
        public String source;

        @McpToolParam(description = "Main table join field.")
        public String target;
    }
}
