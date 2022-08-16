package io.tapdata.autoinspect.connector.pdk;

import com.tapdata.constant.HazelcastUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import io.tapdata.autoinspect.connector.IConnector;
import io.tapdata.autoinspect.connector.IDataCursor;
import io.tapdata.autoinspect.entity.CompareEvent;
import io.tapdata.autoinspect.entity.CompareRecord;
import io.tapdata.flow.engine.V2.entity.PdkStateMap;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.monitor.PDKMethod;
import io.tapdata.schema.PdkTableMap;
import io.tapdata.schema.TapTableUtil;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/12 11:31 Create
 */
public class PdkConnector implements IConnector {
    private static final Logger logger = LogManager.getLogger(PdkConnector.class);
    private static final String TAG = PdkConnector.class.getSimpleName();

    private final Connections connections;
    private final ConnectorNode connectorNode;
    private final Supplier<Boolean> isRunning;

    public PdkConnector(ClientMongoOperator clientMongoOperator, @NonNull DatabaseNode node, @NonNull Connections connections, @NonNull DatabaseTypeEnum.DatabaseType sourceDatabaseType, Supplier<Boolean> isRunning) {
        this.isRunning = isRunning;
        this.connections = connections;
        this.connectorNode = PdkUtil.createNode(
                node.getTaskId(),
                sourceDatabaseType,
                clientMongoOperator,
                getClass().getSimpleName() + "-" + node.getId(),
                connections.getConfig(),
                new PdkTableMap(TapTableUtil.getTapTableMapByNodeId(node.getId())),
                new PdkStateMap(node.getId(), HazelcastUtil.getInstance()),
                PdkStateMap.globalStateMap(HazelcastUtil.getInstance())
        );
        PDKInvocationMonitor.invoke(connectorNode, PDKMethod.INIT, connectorNode::connectorInit, TAG);
    }

    @Override
    public ObjectId getConnId() {
        return new ObjectId(connections.getId());
    }

    @Override
    public String getName() {
        return connections.getName();
    }

    @Override
    public IDataCursor<CompareRecord> queryAll(String tableName) {
        return new PdkQueryCursor(connectorNode, tableName, isRunning);
    }

    @Override
    public void increment(Function<List<CompareEvent>, Boolean> compareEventConsumer) {
        logger.warn("un support");
    }

    @Override
    public CompareRecord queryByKey(String tableName, Map<String, Object> keymap) {
        return null;
    }

    @Override
    public void close() throws Exception {
        if (null != connectorNode) {
            PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP, connectorNode::connectorStop, TAG);
            PDKIntegration.releaseAssociateId(connectorNode.getAssociateId());
        }
    }
}
