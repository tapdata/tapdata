package io.tapdata.services;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.ReflectionUtil;
import io.tapdata.flow.engine.V2.entity.EmptyMap;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.schema.EmptyTapTableMap;
import io.tapdata.service.skeleton.annotation.RemoteService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;

import java.util.Map;
import java.util.UUID;

@RemoteService
@Slf4j
public class DataSourceExecuteService {

    public boolean execute(String connectionId, String commandType, String command) {
        String associateId = "execute_" + connectionId + "_" + UUID.randomUUID();

        try {
            ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
            Connections connections = HazelcastTaskService.taskService().getConnection(connectionId);
            DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, connections.getPdkHash());
            ConnectorNode connectorNode = createConnectorNode(associateId, (HttpClientMongoOperator) clientMongoOperator, databaseType, connections.getConfig());
            String TAG = this.getClass().getSimpleName();
            try {
                PDKInvocationMonitor.invoke(connectorNode, PDKMethod.INIT, connectorNode::connectorInit, TAG);
                switch (commandType) {
                    case "method":
                        Object obj = ReflectionUtil.invokeDeclaredMethod(connectorNode.getConnector(), command.substring(command.lastIndexOf("#") + 1), null);
                        break;
                    case "execute":
                    case "query":
                        break;
                }
            } catch (Exception e) {
                log.error("Failed to init pdk connector, database type: " + databaseType + ", message: " + e.getMessage(), e);
            } finally {
                try {
                    PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP, connectorNode::connectorStop, TAG);
                } catch (Exception e) {
                    log.error(" Stop error{}", e.getMessage());
                }
            }
            return true;
        } finally {
            PDKIntegration.releaseAssociateId(associateId);
        }
    }

    private ConnectorNode createConnectorNode(String associateId, HttpClientMongoOperator clientMongoOperator, DatabaseTypeEnum.DatabaseType databaseType, Map<String, Object> connectionConfig) {
        try {
            PdkUtil.downloadPdkFileIfNeed(clientMongoOperator,
                    databaseType.getPdkHash(), databaseType.getJarFile(), databaseType.getJarRid());
            PDKIntegration.ConnectorBuilder<ConnectorNode> connectorBuilder = PDKIntegration.createConnectorBuilder()
                    .withDagId(associateId)
                    .withAssociateId(associateId)
                    .withConfigContext(null)
                    .withGroup(databaseType.getGroup())
                    .withVersion(databaseType.getVersion())
                    .withPdkId(databaseType.getPdkId())
                    .withTableMap(new EmptyTapTableMap())
                    .withStateMap(new EmptyMap())
                    .withGlobalStateMap(new EmptyMap());
            if (MapUtils.isNotEmpty(connectionConfig)) {
                connectorBuilder.withConnectionConfig(DataMap.create(connectionConfig));
            }
            return connectorBuilder.build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create pdk connector node, database type: " + databaseType + ", message: " + e.getMessage(), e);
        }
    }

}
