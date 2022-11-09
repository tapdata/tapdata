package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVMapFactory;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableV2Function;
import io.tapdata.pdk.apis.functions.connector.target.DropTableFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.workflow.engine.DataFlowWorker;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.target.DMLTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;

import static io.tapdata.entity.simplify.TapSimplify.field;
import static io.tapdata.entity.simplify.TapSimplify.table;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Long;


@DisplayName("Tests for source beginner test")
public class WriteRecordTest extends PDKTestBase {
    private static final String TAG = DMLTest.class.getSimpleName();
    ConnectorNode tddTargetNode;
    ConnectorNode sourceNode;
    DataFlowWorker dataFlowWorker;
    String targetNodeId = "t2";
    String testSourceNodeId = "ts1";
    String originToSourceId;
    TapNodeInfo tapNodeInfo;
    String testTableId;
    @Test
    @DisplayName("Test method handleRead")
    void sourceTest() throws Throwable {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            tapNodeInfo = nodeInfo;
            originToSourceId = "QueryByAdvanceFilterTest_tddSourceTo" + nodeInfo.getTapNodeSpecification().getId();
            testTableId = UUID.randomUUID().toString();
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
            kvMap.put(testTableId,table(testTableId)
                    .add(field("id", JAVA_Long).isPrimaryKey(true).primaryKeyPos(1))
                    .add(field("name", "StringMinor"))
                    .add(field("text", "StringMinor")));
            ConnectorNode connectorNode = PDKIntegration.createConnectorBuilder()
                    .withDagId(dagId)
                    .withAssociateId(UUID.randomUUID().toString())
                    .withConnectionConfig(connectionOptions)
                    .withGroup(spec.getGroup())
                    .withVersion(spec.getVersion())
                    .withTableMap(kvMap)
                    .withPdkId(spec.getId())
                    .withGlobalStateMap(stateMap)
                    .withStateMap(stateMap)
                    .withTable(testTableId)
                    .build();
            TapConnectorContext connectionContext = new TapConnectorContext(
                    spec,
                    connectionOptions,
                    new DataMap());
            try {
                PDKInvocationMonitor.invoke(connectorNode, PDKMethod.INIT,connectorNode::connectorInit,"Init PDK","TEST mongodb");
                writeRecorde(connectionContext,connectorNode);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }finally {
                if (null != connectorNode){
                    PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP,connectorNode::connectorStop,"Stop PDK","TEST mongodb");
                    PDKIntegration.releaseAssociateId("releaseAssociateId");
                }
            }
        });
        waitCompleted(5000000);
    }


    private void writeRecorde(TapConnectorContext connectionContext,ConnectorNode connectorNode) throws Throwable {
        Record[] records = Record.testStart(10);
        RecordEventExecute recordEventExecute = RecordEventExecute.create(connectorNode,connectionContext, this)
                .builderRecord(records);

        recordEventExecute.createTable();

        recordEventExecute.insert();

        for (Record record : records) {
            record.builder("name","Gavin pro").builder("text","Gavin pro max-modify");
        }
        recordEventExecute.update();

        recordEventExecute.delete();

        recordEventExecute.dropTable();
    }

    private void initConnectorFunctions() {
        tddTargetNode = dataFlowWorker.getTargetNodeDriver(targetNodeId).getTargetNode();
        sourceNode = dataFlowWorker.getSourceNodeDriver(testSourceNodeId).getSourceNode();
    }

    public static List<SupportFunction> testFunctions() {
        return Arrays.asList(
                support(WriteRecordFunction.class, "WriteRecord is a must to verify batchRead and streamRead, please implement it in registerCapabilities method."),
                support(CreateTableV2Function.class,"Create table is must to verify ,please implement CreateTableV2Function in registerCapabilities method."),
                support(DropTableFunction.class,"Drop table is must to verify ,please implement DropTableFunction in registerCapabilities method.")
                //support(QueryByAdvanceFilterFunction.class, "QueryByAdvanceFilterFunction is a must for database which is schema free to sample some record to generate the field data types.")
                //support(DropTableFunction.class, "DropTable is needed for TDD to drop the table created by tests, please implement it in registerCapabilities method.")
        );
    }
    public void tearDown() {
        super.tearDown();
    }
}
