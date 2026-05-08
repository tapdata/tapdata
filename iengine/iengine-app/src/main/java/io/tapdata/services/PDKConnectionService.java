package io.tapdata.services;

import com.tapdata.constant.BeanUtil;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.pdk.apis.functions.connection.CommandCallbackFunction;
import io.tapdata.pdk.apis.functions.connection.ConnectorWebsiteFunction;
import io.tapdata.pdk.apis.functions.connection.TableWebsiteFunction;
import io.tapdata.pdk.apis.functions.connection.vo.Website;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectionNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.service.skeleton.annotation.RemoteService;
import io.tapdata.services.util.ConnectionNodeUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
            ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
            ConnectionNode connectorNode = ConnectionNodeUtil.createConnectionNode(clientMongoOperator, connectionId, associateId);
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
            ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
            ConnectionNode connectorNode = ConnectionNodeUtil.createConnectionNode(clientMongoOperator, connectionId, associateId);
            //TableWebsiteFunction
            TableWebsiteFunction tableWebsite = connectorNode.getConnectionFunctions().getTableWebsiteFunction();
            return tableWebsite.getUrl(connectorNode.getConnectionContext(), tables);
        } finally {
            PDKIntegration.releaseAssociateId(associateId);
        }
    }

    public Map<String, String> databaseLogInfoService(String connectionId) {
        if (null == connectionId || "".equals(connectionId.trim())) {
            PDKConnectionService.log.debug("connection id is empty, cancel to get database log info");
            return new HashMap<>();
        }
        String associateId = "DatabaseLogInfoService_" + connectionId + "_" + UUID.randomUUID();
        try {
            ClientMongoOperator clientMongoOperator = BeanUtil.getBean(ClientMongoOperator.class);
            ConnectionNode connectorNode = ConnectionNodeUtil.createConnectionNode(clientMongoOperator, connectionId, associateId);
            TapConnectionContext connectionContext = connectorNode.getConnectionContext();
            if (null == connectionContext) return new HashMap<>();
            DataMap connectionConfig = connectionContext.getConnectionConfig();
            TapNodeSpecification specification = connectionContext.getSpecification();
            if (null == specification) return new HashMap<>();
            List<String> tags = specification.getTags();
            if (null == tags || tags.isEmpty()) return new HashMap<>();
            if (tags.contains("DatabaseLogInfo")) {
                CommandCallbackFunction callback = connectorNode.getConnectionFunctions().getCommandCallbackFunction();
                if (null != callback) {
                    CommandInfo commandInfo = new CommandInfo();
                    commandInfo.setCommand("DatabaseLogInfoService");
                    commandInfo.setConnectionConfig(connectionConfig);
                    CommandResult filter = callback.filter(connectionContext, commandInfo);
                    return null != filter && null != filter.getData() && filter.getData() instanceof Map ?
                            (Map<String, String>) filter.getData() : new HashMap<>();
                } else {
                    return new HashMap<>();
                }
            } else {
                return new HashMap<>();
            }
        } catch (Exception e) {
            PDKConnectionService.log.warn(e.getMessage());
            return new HashMap<>();
        } finally {
            PDKIntegration.releaseAssociateId(associateId);
        }
    }
}
