package io.tapdata.pdk.unit.test;

import com.tapdata.manager.common.utils.StringUtils;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVMapFactory;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.connector.TapConnector;
import io.tapdata.pdk.core.connector.TapConnectorManager;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.utils.CommonUtils;
import io.tapdata.pdk.tdd.tests.support.Record;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.util.*;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static org.junit.Assert.fail;

/**
 * Author:Skeet
 * Date: 2023/6/14
 **/
public class TestYashanWriteRecord {
    private static final String TAG = TestYashanWriteRecord.class.getSimpleName();

    @Test
    public void main() {

//        String testConfig = "src/main/resources/config/yashandb.json";//CommonUtils.getProperty("pdk_test_config_file", "");
//        File testConfigFile = new File(testConfig);
//        if (!testConfigFile.isFile())
//            throw new IllegalArgumentException("TDD test config file doesn't exist or not a file, please check " + testConfigFile);

        String jarUrl = "D:\\work\\tapdata\\tapdata\\connectors\\dist\\yashandb-connector-v1.0-SNAPSHOT.jar";//CommonUtils.getProperty("pdk_test_jar_file", "");
        String tddJarUrl = CommonUtils.getProperty("pdk_external_jar_path", "connectors/dist") + "/yashandb-connector-v1.0-SNAPSHOT.jar";
        File tddJarFile = new File("D:\\work\\tapdata\\tapdata\\connectors\\dist\\tdd-connector-v1.0-SNAPSHOT.jar");
        if (!tddJarFile.isFile())
            throw new IllegalArgumentException("TDD jar file doesn't exist or not a file, please check " + tddJarFile.getAbsolutePath());

        if (StringUtils.isBlank(jarUrl))
            throw new IllegalArgumentException("Please specify jar file in env properties or java system properties, key is pdk_test_jar_file");
        File jarFile = new File(jarUrl);
        if (!jarFile.isFile())
            throw new IllegalArgumentException("PDK jar file " + jarFile.getAbsolutePath() + " is not a file or not exists");
//        TapConnectorManager.getInstance().start();
        TapConnectorManager connectorManager = TapConnectorManager.getInstance();
        connectorManager.start(Arrays.asList(jarFile, tddJarFile));
        connectorManager.refreshJars(jarUrl);
        TapConnector testConnector = TapConnectorManager.getInstance().getTapConnectorByJarName(jarFile.getName());
        Collection<TapNodeInfo> tapNodeInfoCollection = testConnector.getTapNodeClassFactory().getConnectorTapNodeInfos();
        for (TapNodeInfo nodeInfo : tapNodeInfoCollection) {
            TapNodeSpecification specification = nodeInfo.getTapNodeSpecification();
            String iconPath = specification.getIcon();
            TapLogger.info(TAG, "Found connector name {} id {} group {} version {} icon {}", specification.getName(), specification.getId(), specification.getGroup(), specification.getVersion(), specification.getIcon());
            if (StringUtils.isNotBlank(iconPath)) {
                InputStream is = nodeInfo.readResource(iconPath);
                if (is == null) {
                    TapLogger.error(TAG, "Icon image file not found for url {} which defined in spec json file.");
                }
            }
        }
        if (tapNodeInfoCollection.isEmpty())
            throw new CoreException(PDKRunnerErrorCodes.TDD_TAPNODEINFO_NOT_FOUND, "No connector or processor is found in jar " + jarFile);
        String pdkId = null;
        if (pdkId == null) {
            pdkId = CommonUtils.getProperty("pdk_test_pdk_id", null);
            if (pdkId == null)
                fail("Test pdkId is not specified");
        }

        //@TODO
        String tableID = "table";
        TapTable tapTable = Record.testTable(tableID);
        //@TODO
        Record[] insertRecords = Record.testRecordWithTapTable(tapTable, 100000);
        Record[] updateRecords = Record.testRecordWithTapTable(tapTable, 100000);
        List<TapRecordEvent> eventList = new ArrayList<>();
        for (Record record : insertRecords) {
            eventList.add(insertRecordEvent(record, tableID).referenceTime(System.currentTimeMillis()));
        }

        DataMap connectionConfig = new DataMap();
        connectionConfig.put("host","119.23.65.155");
        connectionConfig.put("port",1688);
        connectionConfig.put("user","tapdata");
        connectionConfig.put("password","tapdata");
        connectionConfig.put("schema","TAPDATA");

        DataMap nodeConfig = new DataMap();

        ConnectorNode connectorNode = null;

        for (TapNodeInfo nodeInfo : tapNodeInfoCollection) {
            if (nodeInfo.getTapNodeSpecification().getId().equals(pdkId)) {
                KVMap<Object> stateMap = new KVMap<Object>() {
                    @Override
                    public void init(String mapKey, Class<Object> valueClass) {
                    }

                    @Override
                    public void put(String key, Object o) {
                    }

                    @Override
                    public Object putIfAbsent(String key, Object o) {
                        return null;
                    }

                    @Override
                    public Object remove(String key) {
                        return null;
                    }

                    @Override
                    public void clear() {
                    }

                    @Override
                    public void reset() {
                    }

                    @Override
                    public Object get(String key) {
                        return null;
                    }
                };
                String dagId = UUID.randomUUID().toString();
                KVMap<TapTable> kvMap = InstanceFactory.instance(KVMapFactory.class).getCacheMap(dagId, TapTable.class);
                TapNodeSpecification spec = nodeInfo.getTapNodeSpecification();
                kvMap.put(tableID, tapTable);
                connectorNode = PDKIntegration.createConnectorBuilder()
                        .withDagId(dagId)
                        .withAssociateId(UUID.randomUUID().toString())
                        .withConnectionConfig(connectionConfig)
                        .withNodeConfig(nodeConfig)
                        .withGroup(spec.getGroup())
                        .withVersion(spec.getVersion())
                        .withTableMap(kvMap)
                        .withPdkId(spec.getId())
                        .withGlobalStateMap(stateMap)
                        .withStateMap(stateMap)
                        .withLog(new TapLog())
                        .withTable(tableID)
                        .build();
                break;
            }
        }
        if (null != connectorNode){

            try {
                PDKInvocationMonitor.invoke(connectorNode,
                        PDKMethod.INIT,
                        connectorNode::connectorInit,
                        "Init PDK",  " connector"
                );
                ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();

                try {
                    connectorFunctions.getWriteRecordFunction().writeRecord(
                            connectorNode.getConnectorContext(),
                            eventList,
                            tapTable,
                            consumer -> {
                                consumer.getInsertedCount();
                            }

                    );
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
                throw new RuntimeException(e.getMessage());
            } finally {
                PDKInvocationMonitor.invoke(connectorNode,
                        PDKMethod.STOP,
                        connectorNode::connectorStop,
                        "Stop PDK","  connector"
                );
                PDKIntegration.releaseAssociateId("releaseAssociateId");
            }

        }
    }
}
