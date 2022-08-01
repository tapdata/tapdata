package com.tapdata.tm.task.utils;

import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.CapabilityEnum;
import com.tapdata.tm.commons.util.CreateTypeEnum;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.Lists;
import io.tapdata.pdk.apis.entity.Capability;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;

/**
 * 心跳打点任务配置工具
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/28 11:43 Create
 */
public class ConnHeartbeatUtils {

    public static final String PDK_ID = "dummy";
    public static final String PDK_NAME = "Dummy";
    public static final String MODE = "ConnHeartbeat";
    public static final String CONNECTION_NAME = "tapdata_heartbeat_dummy_connection";
    public static final String TABLE_NAME = "_tapdata_heartbeat_table";

    public static boolean canHeartbeat(SubTaskDto taskDto) {
        return canHeartbeat(taskDto.getParentTask());
    }

    public static boolean canHeartbeat(TaskDto taskDto) {
        return StringUtils.containsAny(taskDto.getSyncType(), TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC)  //同步类型为：迁移、同步
                || ParentTaskDto.TYPE_INITIAL_SYNC.equals(taskDto.getType()) //过滤全量任务
                ;
    }

    public static boolean canHeartbeat(DataSourceConnectionDto connectionDto) {
        // 连接支持增量，且非 dummy
        if (PDK_NAME.equals(connectionDto.getDatabase_type())) {
            return false;
        }
        Set<String> capabilitySet = new HashSet<>();
        for (Capability capability : connectionDto.getCapabilities()) {
            if (StringUtils.containsAny(capability.getId()
                    , CapabilityEnum.STREAM_READ_FUNCTION.name().toLowerCase() //支持增量
                    , CapabilityEnum.CREATE_TABLE_FUNCTION.name().toLowerCase() //能建表
                    , CapabilityEnum.WRITE_RECORD_FUNCTION.name().toLowerCase() //支持写入
            )) {
                capabilitySet.add(capability.getId());
            }
        }
        return capabilitySet.size() == 3;
    }

    public static DataSourceConnectionDto queryConnection(DataSourceService dataSourceService, UserDetail user) {
        //获取打点的Dummy数据源
        Query query2 = new Query(Criteria.where("database_type").is(PDK_NAME)
                .and("createType").is(CreateTypeEnum.System)
                .and("config.mode").is(MODE)
        );
        return dataSourceService.findOne(query2, user);
    }

    public static TaskDto queryTask(TaskService taskService, UserDetail user, List<String> ids) {
        Criteria criteria1 = Criteria.where("is_deleted").is(false).and("syncType").is(TaskDto.SYNC_TYPE_CONN_HEARTBEAT).and("dag.nodes").elemMatch(Criteria.where("connectionId").in(ids));
        Query query1 = new Query(criteria1);
        query1.fields().include("dag", "status", "heartbeatTasks");
        TaskDto oldConnHeartbeatTask = taskService.findOne(query1, user);
        return oldConnHeartbeatTask;
    }

    public static DataSourceConnectionDto generateConnection(DataSourceDefinitionService dataSourceDefinitionService, DataSourceService dataSourceService, UserDetail user, String connId) {
        Query query3 = new Query(Criteria.where("pdkId").is(PDK_ID));
        query3.fields().include("pdkHash", "type");
        DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.findOne(query3);

        DataSourceConnectionDto connection = new DataSourceConnectionDto();
        connection.setName(CONNECTION_NAME);
        connection.setConfig(Optional.of(new LinkedHashMap<String, Object>()).map(m -> {
            m.put("mode", MODE);
            m.put("connId", connId);
            return m;
        }).get());
        connection.setConnection_type("source");
        connection.setPdkType("pdk");
        connection.setRetry(0);
        connection.setStatus("testing");
        connection.setShareCdcEnable(false);
        connection.setDatabase_type(definitionDto.getType());
        connection.setPdkHash(definitionDto.getPdkHash());
        connection.setCreateType(CreateTypeEnum.System);
        connection = dataSourceService.add(connection, user);
        dataSourceService.sendTestConnection(connection, true, true, user); //添加后没加载模型，手动加载一次
        return connection;
    }

    public static TaskDto generateTask(DataSourceConnectionDto heartbeatConnection, DataSourceConnectionDto dataSource, String subTaskId) {
        TableNode sourceNode = new TableNode();
        sourceNode.setId(UUID.randomUUID().toString());
        sourceNode.setTableName(TABLE_NAME);
        sourceNode.setConnectionId(heartbeatConnection.getId().toHexString());
        sourceNode.setDatabaseType(heartbeatConnection.getDatabase_type());
        sourceNode.setName(TABLE_NAME);

        TableNode targetNode = new TableNode();
        targetNode.setId(UUID.randomUUID().toString());
        targetNode.setTableName(TABLE_NAME);
        targetNode.setConnectionId(dataSource.getId().toHexString());
        targetNode.setDatabaseType(dataSource.getDatabase_type());
        targetNode.setName(TABLE_NAME);

        TaskDto taskDto = new TaskDto();
        taskDto.setName("来自" + dataSource.getName() + "的打点任务");
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
