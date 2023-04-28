package io.tapdata.services;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.ConnectionUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.apis.functions.connection.ConnectorWebsiteFunction;
import io.tapdata.pdk.apis.functions.connection.TableWebsiteFunction;
import io.tapdata.pdk.apis.functions.connection.vo.Website;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.service.skeleton.annotation.RemoteService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;

import java.util.List;
import java.util.UUID;

/**
 * @author GavinXiao
 * @description PDKConnectionService create by Gavin
 * @create 2023/4/27 16:31
 **/
@RemoteService
@Slf4j
public class PDKConnectionService {

    public Website getConnectorWebsite(String connectionId) {
        String associateId = "GetConnectorWebsite_" + connectionId + "_" + UUID.randomUUID();
        try {
            ConnectionNode connectorNode = createConnectionNode(connectionId, associateId);
            //ConnectorWebsiteFunction
            ConnectorWebsiteFunction tableWebsite = connectorNode.getConnectionFunctions().getConnectorWebsiteFunction();
            return tableWebsite.getUrl(connectorNode.getConnectionContext());
        } finally {
            PDKIntegration.releaseAssociateId(associateId);
        }
    }

    public Website getTableWebsite(String connectionId, List<String> tables) {
        String associateId = "GetTableWebsite_" + connectionId + "_" + UUID.randomUUID();
        try {
            ConnectionNode connectorNode = createConnectionNode(connectionId, associateId);
            //TableWebsiteFunction
            TableWebsiteFunction tableWebsite = connectorNode.getConnectionFunctions().getTableWebsiteFunction();
            return tableWebsite.getUrl(connectorNode.getConnectionContext(), tables);
        } finally {
            PDKIntegration.releaseAssociateId(associateId);
        }
    }

    private ConnectionNode createConnectionNode(String connectionId, String associateId) {
        ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
        Connections connections = HazelcastTaskService.taskService().getConnection(connectionId);
        if (connections == null)
            throw new CoreException("Connection is empty for id {}.", connectionId);
        DatabaseTypeEnum.DatabaseType databaseType = ConnectionUtil.getDatabaseType(clientMongoOperator, connections.getPdkHash());
        if (databaseType == null)
            throw new CoreException("DatabaseType is null for pdkHash {}, by connection which id is {}.", connectionId, connections.getPdkHash());

        try {
            PdkUtil.downloadPdkFileIfNeed((HttpClientMongoOperator) clientMongoOperator, databaseType.getPdkHash(), databaseType.getJarFile(), databaseType.getJarRid());
            PDKIntegration.ConnectionBuilder<ConnectionNode> connectorBuilder = PDKIntegration.createConnectionConnectorBuilder()
                    .withAssociateId(associateId)
                    .withGroup(databaseType.getGroup())
                    .withVersion(databaseType.getVersion())
                    .withPdkId(databaseType.getPdkId());
            if (MapUtils.isNotEmpty(connections.getConfig())) {
                connectorBuilder.withConnectionConfig(DataMap.create(connections.getConfig()));
            }
            return connectorBuilder.build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create pdk connection node, database type: " + databaseType + ", message: " + e.getMessage(), e);
        }
    }
}
