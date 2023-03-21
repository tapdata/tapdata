package com.tapdata.tm.commons.util;

import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.pdk.apis.entity.Capability;
import org.apache.commons.lang3.StringUtils;
import org.springframework.lang.NonNull;

import java.util.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/2 14:54 Create
 */
public class ConnHeartbeatUtils {
    public static final String PDK_ID = "dummy";
    public static final String PDK_NAME = "Dummy";
    public static final String MODE = "ConnHeartbeat";
    public static final String CONNECTION_NAME = "tapdata_heartbeat_dummy_connection";
    public static final String TABLE_NAME = "_tapdata_heartbeat_table";
    public static final String TASK_RELATION_FIELD = "heartbeatTasks";

    /**
     * check the task need to start heartbeat task
     *
     * @param taskType     task type
     * @param taskSyncType task syncType
     * @return can start heartbeat
     */
    public static boolean checkTask(@NonNull String taskType, @NonNull String taskSyncType) {
        return StringUtils.containsAny(taskSyncType, TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC)  //syncType is migrate or sync
                        || !ParentTaskDto.TYPE_INITIAL_SYNC.equals(taskType) //task type is not initial_sync
        ;
    }

    /**
     * check the connection can be start heartbeat task
     *
     * @param sourceConnectionDto source connection DTO
     * @return can start heartbeat
     */
    public static boolean checkConnection(@NonNull DataSourceConnectionDto sourceConnectionDto) {
        if (!Boolean.TRUE.equals(sourceConnectionDto.getHeartbeatEnable())
                || PDK_NAME.equals(sourceConnectionDto.getDatabase_type())
                || null == sourceConnectionDto.getCapabilities()
                || "source".equalsIgnoreCase(sourceConnectionDto.getConnection_type())
                || "target".equalsIgnoreCase(sourceConnectionDto.getConnection_type())
        ) {
            return false;
        }

        boolean hasStreamRead = false, hasCreateTable = false, hasWriteRecord = false;
        for (Capability capability : sourceConnectionDto.getCapabilities()) {
            if (CapabilityEnum.STREAM_READ_FUNCTION.name().equalsIgnoreCase(capability.getId())) {
                hasStreamRead = true;
            } else if (CapabilityEnum.CREATE_TABLE_FUNCTION.name().equalsIgnoreCase(capability.getId())
                    || CapabilityEnum.CREATE_TABLE_V2_FUNCTION.name().equalsIgnoreCase(capability.getId())) {
                hasCreateTable = true;
            } else if (CapabilityEnum.WRITE_RECORD_FUNCTION.name().equalsIgnoreCase(capability.getId())) {
                hasWriteRecord = true;
            }
        }
        return hasStreamRead && hasCreateTable && hasWriteRecord;
    }

    /**
     * generate heartbeat task
     *
     * @param subTaskId             from task id
     * @param connectionId          from task source connection id
     * @param connectionName        from task source connection name
     * @param databaseType          from task source database type
     * @param heartbeatConnectionId heartbeat connection id
     * @param heartbeatDatabaseType heartbeat database type
     * @return heartbeat task
     */
    public static TaskDto generateTask(@NonNull String subTaskId
            , @NonNull String connectionId, @NonNull String connectionName, @NonNull String databaseType, @NonNull String connectionPdkHash
            , @NonNull String heartbeatConnectionId, @NonNull String heartbeatDatabaseType, @NonNull String heartbeatPdkHash) {
        TableNode sourceNode = new TableNode();
        sourceNode.setId(UUID.randomUUID().toString());
        sourceNode.setTableName(TABLE_NAME);
        sourceNode.setConnectionId(heartbeatConnectionId);
        sourceNode.setDatabaseType(heartbeatDatabaseType);
        sourceNode.setAttrs(new HashMap<String, Object>() {{
            put("pdkHash", heartbeatPdkHash);
        }});
        sourceNode.setName(TABLE_NAME);

        TableNode targetNode = new TableNode();
        targetNode.setId(UUID.randomUUID().toString());
        targetNode.setTableName(TABLE_NAME);
        targetNode.setConnectionId(connectionId);
        targetNode.setDatabaseType(databaseType);
        targetNode.setAttrs(new HashMap<String, Object>() {{
            put("pdkHash", connectionPdkHash);
        }});
        targetNode.setName(TABLE_NAME);
        targetNode.setUpdateConditionFields(Collections.singletonList("id"));

        TaskDto taskDto = new TaskDto();
        taskDto.setName("Heartbeat-" + connectionName);
        taskDto.setDag(DAG.build(new Dag(
                Collections.singletonList(new Edge(sourceNode.getId(), targetNode.getId())),
                Arrays.asList(sourceNode, targetNode)
        )));
        taskDto.setType(ParentTaskDto.TYPE_INITIAL_SYNC_CDC);
        taskDto.setSyncType(TaskDto.SYNC_TYPE_CONN_HEARTBEAT);
        taskDto.setHeartbeatTasks(new HashSet<>(Collections.singleton(subTaskId)));
        return taskDto;
    }

    public static DataSourceConnectionDto generateConnections(String dataSourceId, DataSourceDefinitionDto definitionDto) {
        DataSourceConnectionDto heartbeatConnection = new DataSourceConnectionDto();
        heartbeatConnection.setName(ConnHeartbeatUtils.CONNECTION_NAME);
        heartbeatConnection.setStatus(DataSourceConnectionDto.STATUS_READY);
        heartbeatConnection.setConnection_type("source");
        heartbeatConnection.setCreateType(CreateTypeEnum.System);
        heartbeatConnection.setDatabase_type(definitionDto.getType());
        heartbeatConnection.setPdkType(DataSourceDefinitionDto.PDK_TYPE);
        heartbeatConnection.setPdkHash(definitionDto.getPdkHash());
        heartbeatConnection.setRetry(0);
        heartbeatConnection.setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
        heartbeatConnection.setConfig(new LinkedHashMap<String, Object>() {{
            this.put("mode", MODE);
            this.put("connId", dataSourceId);
            this.put("initial_totals", 1);
            this.put("incremental_interval", 10000);
            this.put("incremental_interval_totals", 1);
            this.put("incremental_types", new int[]{2});
            this.put("table_name", TABLE_NAME);
            this.put("table_fields", new ArrayList<Map<String, Object>>() {{
                add(new HashMap<String, Object>() {{
                    this.put("pri", true);
                    this.put("name", "id");
                    this.put("type", "string(64)");
                    this.put("def", dataSourceId);
                }});
                add(new HashMap<String, Object>() {{
                    this.put("pri", false);
                    this.put("name", "ts");
                    this.put("type", "now");
                }});
            }});
        }});
        return heartbeatConnection;
    }
}
