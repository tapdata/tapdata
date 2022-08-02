package com.tapdata.tm.commons.util;

import com.sun.istack.internal.NotNull;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.pdk.apis.entity.Capability;
import org.apache.commons.lang3.StringUtils;

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
    public static boolean checkTask(@NotNull String taskType, @NotNull String taskSyncType) {
        return StringUtils.containsAny(taskSyncType, TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC)  //syncType is migrate or sync
                || !ParentTaskDto.TYPE_INITIAL_SYNC.equals(taskType) //task type is not initial_sync
                ;
    }

    /**
     * check the connection can be start heartbeat task
     *
     * @param databaseType database type is not dummy
     * @param capabilities capabilities has StreamRead and CreateTable and WriteRecord
     * @return can start heartbeat
     */
    public static boolean checkConnection(@NotNull String databaseType, @NotNull List<Capability> capabilities) {
        if (PDK_NAME.equals(databaseType)) {
            return false;
        }

        Set<String> capabilitySet = new HashSet<>();
        for (Capability capability : capabilities) {
            if (StringUtils.containsAny(capability.getId()
                    , CapabilityEnum.STREAM_READ_FUNCTION.name().toLowerCase()
                    , CapabilityEnum.CREATE_TABLE_FUNCTION.name().toLowerCase()
                    , CapabilityEnum.WRITE_RECORD_FUNCTION.name().toLowerCase()
            )) {
                capabilitySet.add(capability.getId());
            }
        }
        return capabilitySet.size() == 3;
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
    public static TaskDto generateTask(@NotNull String subTaskId, @NotNull String connectionId, @NotNull String connectionName, @NotNull String databaseType, @NotNull String heartbeatConnectionId, @NotNull String heartbeatDatabaseType) {
        TableNode sourceNode = new TableNode();
        sourceNode.setId(UUID.randomUUID().toString());
        sourceNode.setTableName(TABLE_NAME);
        sourceNode.setConnectionId(heartbeatConnectionId);
        sourceNode.setDatabaseType(heartbeatDatabaseType);
        sourceNode.setName(TABLE_NAME);

        TableNode targetNode = new TableNode();
        targetNode.setId(UUID.randomUUID().toString());
        targetNode.setTableName(TABLE_NAME);
        targetNode.setConnectionId(connectionId);
        targetNode.setDatabaseType(databaseType);
        targetNode.setName(TABLE_NAME);

        TaskDto taskDto = new TaskDto();
        taskDto.setName("来自" + connectionName + "的打点任务");
        taskDto.setDag(DAG.build(new Dag(
                Collections.singletonList(new Edge(sourceNode.getId(), targetNode.getId())),
                Arrays.asList(sourceNode, targetNode)
        )));
        taskDto.setType(ParentTaskDto.TYPE_INITIAL_SYNC_CDC);
        taskDto.setSyncType(TaskDto.SYNC_TYPE_CONN_HEARTBEAT);
        taskDto.setHeartbeatTasks(new HashSet<>());
        taskDto.getHeartbeatTasks().add(subTaskId);
        return taskDto;
    }
}
