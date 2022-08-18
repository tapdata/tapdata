package io.tapdata.autoinspect.runner;

import com.tapdata.constant.ConnectionUtil;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.autoinspect.constants.TaskType;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import io.tapdata.autoinspect.AutoInspectRunner;
import io.tapdata.autoinspect.compare.IAutoCompare;
import io.tapdata.autoinspect.compare.impl.AutoPdkCompare;
import io.tapdata.autoinspect.connector.IPdkConnector;
import io.tapdata.autoinspect.connector.pdk.PdkConnector;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.*;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/18 14:57 Create
 */
public abstract class PdkAutoInspectRunner extends AutoInspectRunner<IPdkConnector, IPdkConnector, IAutoCompare> {
    private final ClientMongoOperator clientMongoOperator;
    private final DatabaseNode sourceNode;
    private final DatabaseNode targetNode;
    private final Map<String, Connections> connectionMap;

    public PdkAutoInspectRunner(String taskId, TaskType taskType, ClientMongoOperator clientMongoOperator, DatabaseNode sourceNode, DatabaseNode targetNode) {
        super(taskId, taskType);
        this.clientMongoOperator = clientMongoOperator;
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.connectionMap = getConnectionsByIds(new HashSet<>(Arrays.asList(
                sourceNode.getConnectionId(),
                targetNode.getConnectionId()
        )));
    }

    @Override
    protected IPdkConnector openSourceConnector() throws Exception {
        Connections conn = connectionMap.computeIfAbsent(sourceNode.getConnectionId(), s -> {
            throw new RuntimeException("create node failed because source connection not found: " + s);
        });
        DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, conn.getPdkHash());
        return new PdkConnector(clientMongoOperator, sourceNode, conn, databaseType, this::isRunning);
    }

    @Override
    protected IPdkConnector openTargetConnector() throws Exception {
        Connections conn = connectionMap.computeIfAbsent(targetNode.getConnectionId(), s -> {
            throw new RuntimeException("create node failed because target connection not found: " + s);
        });
        DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, conn.getPdkHash());
        return new PdkConnector(clientMongoOperator, targetNode, conn, databaseType, this::isRunning);
    }

    @Override
    protected IAutoCompare openAutoCompare(IPdkConnector sourceConnector, IPdkConnector targetConnector) throws Exception {
        return new AutoPdkCompare(clientMongoOperator, sourceConnector, targetConnector);
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
}
