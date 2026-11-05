package com.tapdata.tm.mcp.tools;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.dag.vo.TableRenameTableInfo;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.task.service.TaskSaveService;
import com.tapdata.tm.task.service.TaskService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.ai.mcp.annotation.McpToolParam;
import org.springframework.ai.mcp.annotation.context.McpSyncRequestContext;
import org.springframework.stereotype.Component;

import java.util.*;

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
public class CreateMigrateTask {

    private final McpToolSupport toolSupport;
    private final TaskService taskService;
    private final TaskSaveService taskSaveService;
    private final DataSourceService dataSourceService;

    public CreateMigrateTask(McpToolSupport toolSupport,
                             TaskService taskService,
                             TaskSaveService taskSaveService,
                             DataSourceService dataSourceService) {
        this.toolSupport = toolSupport;
        this.taskService = taskService;
        this.taskSaveService = taskSaveService;
        this.dataSourceService = dataSourceService;
    }

    @McpTool(name = "createMigrateTask", description = "Create a replication task in TapData that copies tables from a source database to a target database.")
    public Map<String, Object> createMigrateTask(
            McpSyncRequestContext context,
            @McpToolParam(description = "Task name.") String taskName,
            @McpToolParam(description = "Source connection id.") String sourceConnectionId,
            @McpToolParam(description = "Source database type, for example mongodb, mysql, oracle, sqlserver, or postgresql.") String sourceDatabaseType,
            @McpToolParam(description = "Target connection id.") String targetConnectionId,
            @McpToolParam(description = "Target database type, for example mongodb, mysql, oracle, sqlserver, or postgresql.") String targetDatabaseType,
            @McpToolParam(description = "Table selection mode: custom or expression.") String migrateTableSelectType,
            @McpToolParam(required = false, description = "Tables to migrate when migrateTableSelectType is custom.") List<String> tableNames,
            @McpToolParam(required = false, description = "Regular expression for selecting tables when migrateTableSelectType is expression. Use .* for all tables.") String tableExpression,
            @McpToolParam(required = false, description = "Prefix added to target table names.") String prefix,
            @McpToolParam(required = false, description = "Suffix added to target table names.") String suffix,
            @McpToolParam(required = false, description = "Text to replace in target table names.") String replaceBefore,
            @McpToolParam(required = false, description = "Replacement text used with replaceBefore. Empty string is allowed.") String replaceAfter,
            @McpToolParam(required = false, description = "Target table name case conversion: empty, toUpperCase, or toLowerCase.") String transferCase,
            @McpToolParam(required = false, description = "Explicit table rename rules.") List<TableRenameRule> tableRenameTableNames,
            @McpToolParam(required = false, description = "Task sync type. Defaults to initial_sync+cdc.") String syncType) {
        UserDetail userDetail = toolSupport.getUserDetail(context);
        String resolvedSyncType = StringUtils.defaultIfBlank(syncType, "initial_sync+cdc");
        String resolvedTransferCase = StringUtils.defaultString(transferCase);
        LinkedHashSet<TableRenameTableInfo> tableRenameInfos = parseTableRenameTableNames(tableRenameTableNames);

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
        if (StringUtils.isNotBlank(resolvedTransferCase) && !"toUpperCase".equals(resolvedTransferCase) && !"toLowerCase".equals(resolvedTransferCase)) {
            throw new RuntimeException("Parameter transferCase must be '', 'toUpperCase' or 'toLowerCase'.");
        }
        if (StringUtils.isBlank(replaceBefore) && replaceAfter != null) {
            throw new RuntimeException("Parameter replaceBefore is required when replaceAfter is provided.");
        }

        DataSourceConnectionDto sourceConnection = dataSourceService.findById(new ObjectId(sourceConnectionId), userDetail);
        DataSourceConnectionDto targetConnection = dataSourceService.findById(new ObjectId(targetConnectionId), userDetail);

        String sourceNodeId = UUID.randomUUID().toString();
        String targetNodeId = UUID.randomUUID().toString();
        String tableRenameNodeId = UUID.randomUUID().toString();

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
        List<Edge> edges = new ArrayList<>();
        if (hasTableRenameConfig(prefix, suffix, replaceBefore, replaceAfter, resolvedTransferCase, tableRenameInfos)) {
            TableRenameProcessNode tableRenameNode = buildTableRenameNode(tableRenameNodeId, prefix, suffix, replaceBefore, replaceAfter, resolvedTransferCase, tableRenameInfos);
            nodes.add(tableRenameNode);
            edges.add(new Edge(sourceNodeId, tableRenameNodeId));
            edges.add(new Edge(tableRenameNodeId, targetNodeId));
        } else {
            edges.add(new Edge(sourceNodeId, targetNodeId));
        }
        nodes.add(targetNode);

        Dag dagDto = new Dag(edges, nodes);
        DAG dag = DAG.build(dagDto);

        TaskDto taskDto = new TaskDto();
        taskDto.setName(taskName);
        taskDto.setDag(dag);
        taskDto.setSyncType(TaskDto.SYNC_TYPE_MIGRATE);
        taskDto.setType(resolvedSyncType);

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
        return response;
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

    private TableRenameProcessNode buildTableRenameNode(String nodeId,
                                                        String prefix,
                                                        String suffix,
                                                        String replaceBefore,
                                                        String replaceAfter,
                                                        String transferCase,
                                                        LinkedHashSet<TableRenameTableInfo> tableRenameTableNames) {
        TableRenameProcessNode node = new TableRenameProcessNode();
        node.setId(nodeId);
        node.setName("Table rename");
        node.setTableNames(Optional.ofNullable(tableRenameTableNames).orElseGet(LinkedHashSet::new));
        node.setPrefix(prefix);
        node.setSuffix(suffix);
        node.setReplaceBefore(replaceBefore);
        node.setReplaceAfter(replaceAfter);
        node.setTransferCase(transferCase);
        return node;
    }

    private boolean hasTableRenameConfig(String prefix,
                                         String suffix,
                                         String replaceBefore,
                                         String replaceAfter,
                                         String transferCase,
                                         LinkedHashSet<TableRenameTableInfo> tableRenameTableNames) {
        return StringUtils.isNotBlank(prefix)
                || StringUtils.isNotBlank(suffix)
                || StringUtils.isNotBlank(replaceBefore)
                || replaceAfter != null
                || StringUtils.isNotBlank(transferCase)
                || (tableRenameTableNames != null && !tableRenameTableNames.isEmpty());
    }

    private LinkedHashSet<TableRenameTableInfo> parseTableRenameTableNames(List<TableRenameRule> tableRenameTableNames) {
        if (tableRenameTableNames == null) {
            return new LinkedHashSet<>();
        }

        LinkedHashSet<TableRenameTableInfo> result = new LinkedHashSet<>();
        for (TableRenameRule item : tableRenameTableNames) {
            String originTableName = item == null ? null : item.originTableName;
            String previousTableName = item == null ? null : item.previousTableName;
            String currentTableName = item == null ? null : item.currentTableName;
            if (StringUtils.isBlank(originTableName)) {
                throw new RuntimeException("Parameter tableRenameTableNames.originTableName is required.");
            }
            if (StringUtils.isBlank(previousTableName)) {
                previousTableName = originTableName;
            }
            if (StringUtils.isBlank(currentTableName)) {
                throw new RuntimeException("Parameter tableRenameTableNames.currentTableName is required.");
            }
            result.add(new TableRenameTableInfo(originTableName, previousTableName, currentTableName));
        }
        return result;
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

    public static class TableRenameRule {
        @McpToolParam(description = "Original source table name.")
        public String originTableName;

        @McpToolParam(required = false, description = "Previous table name before rename. Defaults to originTableName.")
        public String previousTableName;

        @McpToolParam(description = "Current target table name after rename.")
        public String currentTableName;
    }
}
