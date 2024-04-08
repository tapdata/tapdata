package io.tapdata.Runnable;

import com.tapdata.entity.Connections;
import com.tapdata.entity.Schema;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.flow.engine.V2.entity.EmptyMap;
import io.tapdata.modules.api.pdk.PDKUtils;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.schema.EmptyTapTableMap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class KafkaLoadSchemaRunner{
    private final static String TAG = LoadSchemaRunner.class.getSimpleName();
    TableFieldTypesGenerator tableFieldTypesGenerator = InstanceFactory.instance(TableFieldTypesGenerator.class);
    private final static int BATCH_SIZE = 20;
    private Logger logger = LogManager.getLogger(KafkaLoadSchemaRunner.class);
    private Schema schema;
    private Map<String, Object> nodeConfig;
    private Connections connections;
    private ClientMongoOperator clientMongoOperator;
    private List<String> tableName;
    Map<String, Object> tapTableMap = new ConcurrentHashMap<>();

    Map<String, Object> resultMap = new HashMap<>();

    public KafkaLoadSchemaRunner(Map<String, Object> nodeConfig, Connections connections, ClientMongoOperator clientMongoOperator, List<String> tableName) {
        this.nodeConfig = nodeConfig;
        this.connections = connections;
        this.clientMongoOperator=clientMongoOperator;
        this.schema = new Schema();
        List<TapTable> tapTables = new ArrayList<>();
        this.schema.setTapTables(tapTables);
        this.tableName=tableName;
    }

    public Map<String,Object> run() {
        long ts = System.currentTimeMillis();
        ConnectorNode connectorNode = null;
        try {
            String associatedId = connections.getName() + "_" + ts;
//            connectionNode = PDKIntegration.createConnectionConnectorBuilder()
//                    .withConnectionConfig(DataMap.create(connections.getConfig()))
//                    .withNodeConfig(DataMap.create(nodeConfig))
//                    .withGroup(pdkInfo.getGroup())
//                    .withPdkId(pdkInfo.getPdkId())
//                    .withAssociateId(associatedId)
//                    .withVersion(pdkInfo.getVersion())
//                    .withLog(new TapLog())
//                    .build();
            connectorNode = createConnectorNode(associatedId, connections.getConfig(), nodeConfig);
            PDKInvocationMonitor.invoke(connectorNode, PDKMethod.INIT, connectorNode::connectorInit, "Init PDK", TAG);
            DefaultExpressionMatchingMap dataTypesMap = connectorNode.getConnectorContext().getSpecification().getDataTypesMap();
            ConnectorNode finalConnectorNode = connectorNode;
            PDKInvocationMonitor.invoke(connectorNode, PDKMethod.DISCOVER_SCHEMA,
                    () -> {
                            finalConnectorNode.getConnector().discoverSchema(finalConnectorNode.getConnectorContext(), tableName, BATCH_SIZE, tapTables -> comsumeTable(tapTables,dataTypesMap));
                    }, TAG);
            resultMap.put("ts",System.currentTimeMillis());
            resultMap.put("code","success");
            if (MapUtils.isNotEmpty(tapTableMap)) {
                resultMap.put("data",tapTableMap);
            }
            return resultMap;
        } catch (Exception e) {
            resultMap.put("ts",System.currentTimeMillis());
            resultMap.put("code", "error");
            resultMap.put("message", e.getMessage());
            return resultMap;
        }finally {
            if (connectorNode != null)
                PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP, connectorNode::connectorStop, "Stop PDK", TAG);
            PDKIntegration.releaseAssociateId(connections.getName() + "_" + ts);
        }
    }

    private void comsumeTable(List<TapTable> tapTables, DefaultExpressionMatchingMap dataTypesMap) {
        if(CollectionUtils.isEmpty(tapTables)){
            return;
        }
        tapTables.forEach(tapTable -> {
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            if (MapUtils.isNotEmpty(nameFieldMap)) {
                nameFieldMap.forEach((fieldName, tapField) -> {
                    if (null == tapField.getTapType()) {
                        tableFieldTypesGenerator.autoFill(tapField, dataTypesMap);
                    }
                });
            }
            tapTableMap.put(tapTable.getName(),tapTable);
        });
    }

    protected ConnectorNode createConnectorNode(String associateId, Map<String, Object> connectionConfig, Map<String, Object> nodeConfig) {
        try {
            PDKUtils pdkUtils = InstanceFactory.instance(PDKUtils.class);
            PDKUtils.PDKInfo pdkInfo = pdkUtils.downloadPdkFileIfNeed(connections.getPdkHash());
//            PdkUtil.downloadPdkFileIfNeed(clientMongoOperator,
//                    databaseType.getPdkHash(), databaseType.getJarFile(), databaseType.getJarRid());
            PDKIntegration.ConnectorBuilder<ConnectorNode> connectorBuilder = PDKIntegration.createConnectorBuilder()
                    .withDagId(associateId)
                    .withAssociateId(associateId)
                    .withConfigContext(null)
                    .withGroup(pdkInfo.getGroup())
                    .withVersion(pdkInfo.getVersion())
                    .withPdkId(pdkInfo.getPdkId())
                    .withNodeConfig(DataMap.create(nodeConfig))
                    .withTableMap(new EmptyTapTableMap())
                    .withStateMap(new EmptyMap())
                    .withGlobalStateMap(new EmptyMap());
            if (MapUtils.isNotEmpty(connectionConfig)) {
                connectorBuilder.withConnectionConfig(DataMap.create(connectionConfig));
            }
            return connectorBuilder.build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create pdk connector node, database type: "  + ", message: " + e.getMessage(), e);
        }
    }
//    public void tableConsumer(List<TapTable> tables) {
//        if(CollectionUtils.isEmpty(tables)){
//            return;
//        }
//        tables.forEach(tapTable -> {
//            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
//            if (MapUtils.isNotEmpty(nameFieldMap)) {
//                nameFieldMap.forEach((fieldName, tapField) -> {
//                    if (null == tapField.getTapType()) {
//                        tableFieldTypesGenerator.autoFill(tapField, dataTypesMap);
//                    }
//                });
//            }
//            tapTableMap.put(tapTable.getName(),tapTable);
//        });
//
//    }
}
