package io.tapdata.autoinspect;

import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.autoinspect.compare.AutoPdkCompare;
import io.tapdata.autoinspect.compare.IAutoCompare;
import io.tapdata.autoinspect.connector.IConnector;
import io.tapdata.autoinspect.connector.pdk.PdkConnector;
import io.tapdata.autoinspect.status.AutoInspectStatusCtl;
import lombok.NonNull;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.Assert;

import java.util.*;
import java.util.function.Function;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/15 20:10 Create
 */
public class AutoInspectPdkTask extends AutoInspectTask {

    private final ClientMongoOperator clientMongoOperator;
    private final DatabaseNode sourceNode;
    private final DatabaseNode targetNode;
    private final Map<String, Connections> connectionMap;

    public AutoInspectPdkTask(@NonNull AutoInspectStatusCtl statusCtl, @NonNull TaskDto task, @NonNull ClientMongoOperator clientMongoOperator, Function<AutoInspectStatusCtl, Boolean> closedFn) {
        super(statusCtl, closedFn);
        this.clientMongoOperator = clientMongoOperator;

        DAG dag = task.getDag();
        //todo: 复制场景只有一个源和目标
        sourceNode = Optional.ofNullable(dag.getSourceNode()).map(nodes -> {
            DatabaseNode node = null;
            if (!nodes.isEmpty()) {
                node = nodes.get(0);
            }
            return node;
        }).orElse(null);
        targetNode = Optional.ofNullable(dag.getTargetNode()).map(nodes -> {
            DatabaseNode node = null;
            if (!nodes.isEmpty()) {
                node = nodes.get(0);
            }
            return node;
        }).orElse(null);

        Assert.notNull(sourceNode, "source node can not be null");
        Assert.notNull(targetNode, "target node can not be null");

        connectionMap = getConnectionsByIds(new HashSet<>(Arrays.asList(
                sourceNode.getConnectionId(),
                targetNode.getConnectionId()
        )));
    }

    private Map<String, Connections> getConnectionsByIds(Set<String> ids) {
        Query query = Query.query(Criteria.where("_id").in(ids));
        query.fields().exclude("response_body").exclude("schema");
        List<Connections> connections = clientMongoOperator.find(query, ConnectorConstant.CONNECTION_COLLECTION + "/listAll", Connections.class);

        Map<String, Connections> retMap = new HashMap<>();
        if (null != connections) {
            connections.forEach(conn -> {
                retMap.put(conn.getId(), conn);
            });
        }
        return retMap;
    }

    @Override
    protected IConnector openSourceConnector() {
        Connections conn = connectionMap.computeIfAbsent(sourceNode.getConnectionId(), s -> {
            throw new RuntimeException("create node failed because source connection not found: " + s);
        });
        DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, conn.getPdkHash());
        return new PdkConnector(clientMongoOperator, sourceNode, conn, databaseType, this::isRunning);
    }

    @Override
    protected IConnector openTargetConnector() {
        Connections conn = connectionMap.computeIfAbsent(targetNode.getConnectionId(), s -> {
            throw new RuntimeException("create node failed because target connection not found: " + s);
        });
        DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, conn.getPdkHash());
        return new PdkConnector(clientMongoOperator, targetNode, conn, databaseType, this::isRunning);
    }

    @Override
    protected IAutoCompare openAutoCompare(IConnector sourceConnector, IConnector targetConnector) {
        return new AutoPdkCompare(clientMongoOperator, sourceConnector, targetConnector);
    }
}
