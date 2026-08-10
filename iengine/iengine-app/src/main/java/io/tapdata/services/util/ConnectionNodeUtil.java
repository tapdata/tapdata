package io.tapdata.services.util;

import com.tapdata.constant.ConnectionUtil;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.mongo.HttpClientMongoOperator;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import io.tapdata.flow.engine.V2.util.PdkUtil;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import org.apache.commons.collections4.MapUtils;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/8 14:46 Create
 * @description
 */
public final class ConnectionNodeUtil {
    private ConnectionNodeUtil() {

    }

    public static ConnectionNode createConnectionNode(ClientMongoOperator clientMongoOperator, String connectionId, String associateId) {
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
