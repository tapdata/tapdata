package io.tapdata.pdk.performance;

import com.tapdata.manager.common.utils.StringUtils;
import io.tapdata.entity.conversion.TargetTypesGenerator;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVMapFactory;
import io.tapdata.flow.engine.V2.entity.EmptyMap;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableV2Function;
import io.tapdata.pdk.apis.functions.connector.target.DropTableFunction;
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
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_String;
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

        String jarUrl = "/Users/aplomb/dev/code/NewTapdataProjects/tapdata_enterprise/connectors/dist/yashandb-connector-v1.0-SNAPSHOT.jar";//CommonUtils.getProperty("pdk_test_jar_file", "");
//        String tddJarUrl = CommonUtils.getProperty("pdk_external_jar_path", "connectors/dist") + "/yashandb-connector-v1.0-SNAPSHOT.jar";

        if (StringUtils.isBlank(jarUrl))
            throw new IllegalArgumentException("Please specify jar file in env properties or java system properties, key is pdk_test_jar_file");
        File jarFile = new File(jarUrl);
        if (!jarFile.isFile())
            throw new IllegalArgumentException("PDK jar file " + jarFile.getAbsolutePath() + " is not a file or not exists");
//        TapConnectorManager.getInstance().start();
        TapConnectorManager connectorManager = TapConnectorManager.getInstance();
        connectorManager.start(Collections.singletonList(jarFile));
        connectorManager.refreshJars(jarUrl);
        TapNodeInfo nodeInfo = null;
        TapConnector testConnector = TapConnectorManager.getInstance().getTapConnectorByJarName(jarFile.getName());
        Collection<TapNodeInfo> tapNodeInfoCollection = testConnector.getTapNodeClassFactory().getConnectorTapNodeInfos();
        if (tapNodeInfoCollection.isEmpty())
            throw new CoreException(PDKRunnerErrorCodes.TDD_TAPNODEINFO_NOT_FOUND, "No connector or processor is found in jar " + jarFile);
        nodeInfo = tapNodeInfoCollection.iterator().next();

        //@TODO
        String tableID = "table_perf_test";
        TapTable tapTable = table(tableID);
        tapTable.add(field("id", "String").primaryKeyPos(1).tapType(tapString().bytes(50L)));
        int fieldCount = 6;
        int batchCount = 1;
        int recordCount = 1;
        for(int i = 0; i < fieldCount; i++) {
            tapTable.add(field("Type_STRING_" + i, JAVA_String).tapType(tapString().bytes(50L)));
//            tapTable.add(field("Type_NUMBER_" + i, JAVA_Double).tapType(tapNumber().precision(15).scale(2).minValue(BigDecimal.ZERO).maxValue(BigDecimal.valueOf(1000000000000000L))));
        }
//        tapTable
//            .add(field("id", JAVA_Long).isPrimaryKey(true).primaryKeyPos(1).tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
//            .add(field("Type_ARRAY", JAVA_Array).tapType(tapArray()))
//            .add(field("Type_BINARY", JAVA_Binary).tapType(tapBinary().bytes(100L)))
//            .add(field("Type_BOOLEAN", JAVA_Boolean).tapType(tapBoolean()))
//            .add(field("Type_DATE", JAVA_Date).tapType(tapDate()))
//            .add(field("Type_DATETIME", "Date_Time").tapType(tapDateTime().fraction(3)))
//            .add(field("Type_DATETIME_WITH_TIME_ZONE", "Date_Time").tapType(tapDateTime().fraction(3).withTimeZone(true)))
//            .add(field("Type_MAP", JAVA_Map).tapType(tapMap()))
//            .add(field("Type_NUMBER_Long", JAVA_Long).tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
//            .add(field("Type_NUMBER_INTEGER", JAVA_Integer).tapType(tapNumber().maxValue(BigDecimal.valueOf(Integer.MAX_VALUE)).minValue(BigDecimal.valueOf(Integer.MIN_VALUE))))
//            .add(field("Type_NUMBER_BigDecimal", JAVA_BigDecimal).tapType(tapNumber().maxValue(BigDecimal.valueOf(Double.MAX_VALUE)).minValue(BigDecimal.valueOf(-Double.MAX_VALUE)).precision(200).scale(4).fixed(true)))
//            .add(field("Type_NUMBER_Float", JAVA_Float).tapType(tapNumber().maxValue(BigDecimal.valueOf(Float.MAX_VALUE)).minValue(BigDecimal.valueOf(-Float.MAX_VALUE)).precision(200).scale(4).fixed(false)))
//            .add(field("Type_NUMBER_Double", JAVA_Double).tapType(tapNumber().maxValue(BigDecimal.valueOf(Double.MAX_VALUE)).minValue(BigDecimal.valueOf(-Double.MAX_VALUE)).precision(200).scale(4).fixed(false)))
//            .add(field("Type_STRING_1", JAVA_String).tapType(tapString().bytes(50L)))
//            .add(field("Type_STRING_2", JAVA_String).tapType(tapString().bytes(50L)))
//            .add(field("Type_INT64", "INT64").tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
//            .add(field("Type_TIME", "Time").tapType(tapTime().withTimeZone(false)))
//            .add(field("Type_TIME_WITH_TIME_ZONE", "Time").tapType(tapTime().withTimeZone(true)))
//            .add(field("Type_YEAR", "Year").tapType(tapYear()))
//        ;
        //@TODO
//        Record[] updateRecords = Record.testRecordWithTapTable(tapTable, 100000);
        List<List<TapRecordEvent>> eventLists = new ArrayList<>();

        for(int i = 0; i < batchCount; i++) {
            eventLists.add(new ArrayList<>());
        }
        for(List<TapRecordEvent> eventList : eventLists) {
            Record[] insertRecords = Record.testRecordWithTapTable(tapTable, recordCount);
            for (Record record : insertRecords) {
                eventList.add(insertRecordEvent(record, tableID).referenceTime(System.currentTimeMillis()));
            }
        }

        DataMap connectionConfig = new DataMap();
        connectionConfig.put("host","119.23.65.155");
        connectionConfig.put("port",1688);
        connectionConfig.put("user","tapdata");
        connectionConfig.put("password","tapdata");
        connectionConfig.put("schema","TAPDATA");

        DataMap nodeConfig = new DataMap();

        ConnectorNode connectorNode = null;
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
                .withGlobalStateMap(new EmptyMap())
                .withStateMap(new EmptyMap())
                .withLog(new TapLog())
                .withTable(tableID)
                .build();
        if (null != connectorNode){

            try {
                PDKInvocationMonitor.invoke(connectorNode,
                        PDKMethod.INIT,
                        connectorNode::connectorInit,
                        "Init PDK",  " connector"
                );
                ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
                CreateTableV2Function createTableV2Function = connectorFunctions.getCreateTableV2Function();
                if(createTableV2Function != null) {
                    DropTableFunction dropTableFunction = connectorFunctions.getDropTableFunction();
                    if(dropTableFunction != null) {
                        ConnectorNode finalConnectorNode = connectorNode;
                        TapDropTableEvent tapDropTableEvent = dropTableEvent();
                        tapDropTableEvent.setTableId(tableID);
                        CommonUtils.ignoreAnyError(() -> dropTableFunction.dropTable(finalConnectorNode.getConnectorContext(), tapDropTableEvent), "DROP");
                    }

                    TargetTypesGenerator targetTypesGenerator = InstanceFactory.instance(TargetTypesGenerator.class);

                    TapResult<LinkedHashMap<String, TapField>> convertResult = targetTypesGenerator.convert(tapTable.getNameFieldMap(), connectorNode.getTapNodeInfo().getTapNodeSpecification().getDataTypesMap(), connectorNode.getCodecsFilterManager());
                    TapTable table = table(tableID);
                    table.setNameFieldMap(convertResult.getData());
                    TapCreateTableEvent createTableEvent = createTableEvent().table(table);
                    createTableV2Function.createTable(connectorNode.getConnectorContext(), createTableEvent);
                }
                try {
                    LongAdder counter = new LongAdder();
                    long time = System.currentTimeMillis();
                    for(List<TapRecordEvent> eventList : eventLists) {
                        connectorFunctions.getWriteRecordFunction().writeRecord(
                                connectorNode.getConnectorContext(),
                                eventList,
                                tapTable,
                                result -> {
                                    counter.add(result.getInsertedCount());
                                }

                        );
                    }
                    long takes = (System.currentTimeMillis() - time);
                    System.out.println("inserted count: " + counter.longValue() + " fieldCount " + fieldCount + " takes " + takes + "ms" + "--> qps " + (counter.longValue() * 1000 / takes));
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
                throw new RuntimeException(e.getMessage());
            } catch (Throwable e) {
                throw new RuntimeException(e);
            } finally {
                PDKInvocationMonitor.invoke(connectorNode,
                        PDKMethod.STOP,
                        connectorNode::connectorStop,
                        "Stop PDK","  connector"
                );
                PDKIntegration.releaseAssociateId(connectorNode.getAssociateId());
            }

        }
    }
}
