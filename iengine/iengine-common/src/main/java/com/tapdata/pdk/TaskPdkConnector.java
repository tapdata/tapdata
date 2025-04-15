package com.tapdata.pdk;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.entity.task.config.TaskRetryConfig;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.Optional;

/**
 * 任务连接器生成类
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/27 15:09 Create
 */
public class TaskPdkConnector {

    private final String taskId;
    private final TaskDto task;
    private final TaskConfig taskConfig;
    private final ClientMongoOperator clientMongoOperator;

    public TaskPdkConnector(TaskDto task) {
        this.task = task;
        this.taskId = task.getId().toHexString();
        this.taskConfig = getTaskConfig(task);
        this.clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
    }

    public IPdkConnector createSource(String associateId) {
        return createFirstMatch(associateId, true);
    }

    public IPdkConnector createTarget(String associateId) {
        return createFirstMatch(associateId, false);
    }

    public IPdkConnector create(String nodeId, String associateId) {
        Node<?> node = task.getDag().getNode(nodeId);
        return create(node, associateId);
    }

    protected IPdkConnector createFirstMatch(String associateId, boolean isSource) {
        return Optional.ofNullable(task.getDag())
            .map(dag -> isSource ? dag.getSources() : dag.getTargets())
            .map(nodes -> {
                for (Node<?> n : nodes) {
                    if (n instanceof DataParentNode) return n;
                }
                return null;
            }).map(node -> create(node, associateId))
            .orElse(null);
    }

    public IPdkConnector create(Node<?> node, String associateId) {
        if (node instanceof DataParentNode) {
            String connectionId = ((DataParentNode<?>) node).getConnectionId();
            Connections connections = getConnections(connectionId);
            if (null != connections) {
                DatabaseTypeEnum.DatabaseType sourceDatabaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, connections.getPdkHash());
                return TaskNodePdkConnector.create(clientMongoOperator, taskId, node, associateId, connections, sourceDatabaseType, taskConfig.getTaskRetryConfig());
            }
        }
        return null;
    }

    protected TaskConfig getTaskConfig(TaskDto taskDto) {
        long retryIntervalSecond = 5L;
        long maxRetryTimeMinute = 20L;
        long maxRetryTimeSecond = maxRetryTimeMinute * 60;
        TaskRetryConfig taskRetryConfig = TaskRetryConfig.create()
            .retryIntervalSecond(retryIntervalSecond)
            .maxRetryTimeSecond(maxRetryTimeSecond);
        return TaskConfig.create().taskDto(taskDto).taskRetryConfig(taskRetryConfig);
    }

    protected Connections getConnections(String id) {
        Query query = Query.query(Criteria.where("_id").is(id));
        query.fields().exclude("response_body").exclude("schema");
        List<Connections> list = clientMongoOperator.find(query, ConnectorConstant.CONNECTION_COLLECTION + "/listAll", Connections.class);
        if (null != list && !list.isEmpty()) {
            return list.get(0);
        }
        return null;
    }

    public static TaskPdkConnector of(TaskDto task) {
        return new TaskPdkConnector(task);
    }
}
