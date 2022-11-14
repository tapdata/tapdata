package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVMapFactory;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableFunction;
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
import io.tapdata.pdk.tdd.tests.support.Record;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static io.tapdata.entity.simplify.TapSimplify.field;
import static io.tapdata.entity.simplify.TapSimplify.table;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Long;


@DisplayName("Test.WriteRecordTestV2")
public class WriteRecordV2Test extends PDKTestBase {
    private static final String TAG = WriteRecordV2Test.class.getSimpleName();
    ConnectorNode tddTargetNode;
    ConnectorNode sourceNode;
    DataFlowWorker dataFlowWorker;
    String targetNodeId = "t2";
    String testSourceNodeId = "ts1";
    String originToSourceId;
    TapNodeInfo tapNodeInfo;
    String testTableId;
    TapTable targetTable = table(testTableId)
            .add(field("id", JAVA_Long).isPrimaryKey(true).primaryKeyPos(1))
            .add(field("name", "STRING"))
            .add(field("text", "STRING"));
    @Test
    @DisplayName("Test.WriteRecordTestV2.case.sourceTest")
    void sourceTest() throws Throwable {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            tapNodeInfo = nodeInfo;
            originToSourceId = "QueryByAdvanceFilterTest_tddSourceTo" + nodeInfo.getTapNodeSpecification().getId();
            testTableId = UUID.randomUUID().toString();
            targetTable.setId(testTableId);
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
            kvMap.put(testTableId,targetTable);
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

            TapConnectorContext connectionContext = new TapConnectorContext(spec, connectionOptions, new DataMap());

            try {
                PDKInvocationMonitor.invoke(connectorNode, PDKMethod.INIT,connectorNode::connectorInit,"Init PDK","TEST mongodb");
                writeRecorde(connectionContext,connectorNode,this.getMethod("sourceTest"));
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }finally {
                if (null != connectorNode){
                    PDKInvocationMonitor.invoke(connectorNode, PDKMethod.STOP,connectorNode::connectorStop,"Stop PDK","TEST mongodb");
                    PDKIntegration.releaseAssociateId("releaseAssociateId");
                }
            }
        });
        //waitCompleted(5000000);
    }


    long insertRecord;
    long updateRecord;
    long deleteRecord;

    long insertRecordNeed = 10;
    long updateRecordNeed;
    long deleteRecordNeed;

    private void writeRecorde(TapConnectorContext connectionContext, ConnectorNode connectorNode, Method testCase) throws Throwable {
        Record[] records = Record.testStart((int)insertRecordNeed);
        RecordEventExecute recordEventExecute = RecordEventExecute.create(connectorNode,connectionContext, this)
                .testCase(testCase)
                .builderRecord(records);

        //boolean table = recordEventExecute.createTable();

        WriteListResult<TapRecordEvent> insert = recordEventExecute.insert();
        insertRecord = insert.getInsertedCount();
        updateRecordNeed = 0;
        for (Record record : records) {
            record.builder("name","Gavin pro").builder("text","Gavin pro max-modify");
            updateRecordNeed++;
        }
        WriteListResult<TapRecordEvent> update = recordEventExecute.update();
        updateRecord = update.getModifiedCount();

        deleteRecordNeed = insertRecordNeed;
        WriteListResult<TapRecordEvent> delete = recordEventExecute.delete();
        deleteRecord = delete.getRemovedCount();

        recordEventExecute.dropTable();
    }

    private void initConnectorFunctions() {
        tddTargetNode = dataFlowWorker.getTargetNodeDriver(targetNodeId).getTargetNode();
        sourceNode = dataFlowWorker.getSourceNodeDriver(testSourceNodeId).getSourceNode();
    }

    public static List<SupportFunction> testFunctions() {
        List<SupportFunction> supportFunctions = Arrays.asList(
                support(WriteRecordFunction.class, "WriteRecord is a must to verify batchRead and streamRead, please implement it in registerCapabilities method."),
                support(CreateTableFunction.class,"Create table is must to verify ,please implement CreateTableFunction in registerCapabilities method."),
                support(DropTableFunction.class, "Drop table is must to verify ,please implement DropTableFunction in registerCapabilities method.")
                //support(QueryByAdvanceFilterFunction.class, "QueryByAdvanceFilterFunction is a must for database which is schema free to sample some record to generate the field data types.")
                //support(DropTableFunction.class, "DropTable is needed for TDD to drop the table created by tests, please implement it in registerCapabilities method.")
        );
        return supportFunctions;
    }
    public void tearDown() {
        super.tearDown();
        TapLogger.info(TAG, "Test table name : {}, insert: {}/{}, update: {}/{}, delete: {}/{}",
                this.testTableId,
                this.insertRecord,this.insertRecordNeed,
                this.updateRecord,this.updateRecordNeed,
                this.deleteRecord,this.deleteRecordNeed
        );

    }
    @Override
    public Class<? extends PDKTestBase> get() {
        return this.getClass();
    }
}
