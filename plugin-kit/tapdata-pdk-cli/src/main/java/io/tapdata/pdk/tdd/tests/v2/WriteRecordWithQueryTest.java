package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableV2Function;
import io.tapdata.pdk.apis.functions.connector.target.DropTableFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.cli.entity.DAGDescriber;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.dag.TapDAGNode;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.workflow.engine.DataFlowWorker;
import io.tapdata.pdk.core.workflow.engine.JobOptions;
import io.tapdata.pdk.core.workflow.engine.TapDAGNodeEx;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.target.DMLTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


@DisplayName("Tests for source beginner test")
public class WriteRecordWithQueryTest extends PDKTestBase {
    private static final String TAG = DMLTest.class.getSimpleName();
    ConnectorNode tddTargetNode;
    ConnectorNode sourceNode;
    DataFlowWorker dataFlowWorker;
    String targetNodeId = "t2";
    String testTargetNodeId = "tt1";
    String testSourceNodeId = "ts1";
    String originNodeId = "r0";

    String originToSourceId;

    DAGDescriber dataFlowDescriber;

    TapNodeInfo tapNodeInfo;

    String testTableId;
    String tddTableId = "tdd-table";
    @Test
    @DisplayName("Test method handleRead")
    void sourceTest() throws Throwable {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            tapNodeInfo = nodeInfo;
            originToSourceId = "QueryByAdvanceFilterTest_tddSourceTo" + nodeInfo.getTapNodeSpecification().getId();

            TapNodeSpecification spec = nodeInfo.getTapNodeSpecification();
            dataFlowDescriber = new DAGDescriber();
            dataFlowDescriber.setId(originToSourceId);
            testTableId = testTableName(dataFlowDescriber.getId());

            dataFlowDescriber.setNodes(Arrays.asList(
                    new TapDAGNodeEx().id(originNodeId).pdkId("tdd-source").group("io.tapdata.connector").type(TapDAGNode.TYPE_SOURCE).version("1.0-SNAPSHOT").
                            table(tddTableId).connectionConfig(new DataMap()),
                    new TapDAGNodeEx().id(testTargetNodeId).pdkId(spec.getId()).group(spec.getGroup()).type(TapDAGNode.TYPE_TARGET/*nodeInfo.getNodeType()*/).version(spec.getVersion()).
                            table(testTableId).connectionConfig(connectionOptions)
            ));
            dataFlowDescriber.setDag(Collections.singletonList(Arrays.asList(originNodeId, testTargetNodeId)));
            dataFlowDescriber.setJobOptions(new JobOptions().actionsBeforeStart(Arrays.asList(JobOptions.ACTION_DROP_TABLE, JobOptions.ACTION_CREATE_TABLE)).enableStreamRead(false));

            dag = dataFlowDescriber.toDag();

            TapConnectorContext connectionContext = new TapConnectorContext(
                    spec,
                    connectionOptions,
                    new DataMap());
            try{
                PDKInvocationMonitor.invoke(tddTargetNode, PDKMethod.INIT,tddTargetNode::connectorInit,"Init PDK","TEST mongodb");

                queryByAdvanceFilterTest(dataFlowWorker.getTargetNodeDriver(testTargetNodeId).getTargetNode(),connectionContext);

                insertAfterInsertSomeKey(dataFlowWorker.getTargetNodeDriver(testTargetNodeId).getTargetNode(),connectionContext);

                updateNotExistRecord(dataFlowWorker.getTargetNodeDriver(testTargetNodeId).getTargetNode(),connectionContext);

                deleteNotExistRecord(dataFlowWorker.getTargetNodeDriver(testTargetNodeId).getTargetNode(),connectionContext);

            }catch (Throwable e){
                throw new RuntimeException(e);
            }finally {
                if (null != tddTargetNode){
                    PDKInvocationMonitor.invoke(tddTargetNode, PDKMethod.STOP,tddTargetNode::connectorStop,"Stop PDK","TEST mongodb");
                    PDKIntegration.releaseAssociateId("releaseAssociateId");
                }
            }

//            if(dag != null) {
//                dataFlowEngine.startDataFlow(dag, dataFlowDescriber.getJobOptions(), (fromState, toState, dataFlowWorker) -> {
//                    if (toState.equals(DataFlowWorker.STATE_INITIALIZED)) {
//                        dataFlowEngine.sendExternalTapEvent(originToSourceId, new PatrolEvent().patrolListener((nodeId, state) -> {
//                            TapLogger.debug("PATROL STATE_INITIALIZED", "NodeId {} state {}", nodeId, (state == PatrolEvent.STATE_ENTER ? "enter" : "leave"));
//                            if (nodeId.equals(testTargetNodeId) && state == PatrolEvent.STATE_LEAVE) {
//                                for (int i = 0; i < 10; i++) {
//                                    DataMap dataMap = buildInsertRecord();
//                                    dataMap.put("id", "id_" + i);
//                                    sendInsertRecordEvent(dataFlowEngine, dag, tddTableId, dataMap);
//                                    lastRecordToEqual = dataMap;
//                                }
//                                sendPatrolEvent(dataFlowEngine, dag, new PatrolEvent().patrolListener((innerNodeId, innerState) -> {
//                                    if (innerNodeId.equals(testTargetNodeId) && innerState == PatrolEvent.STATE_LEAVE) {

//                                    }
//                                }));
//                            }
//                        }));
//                    }
//                });
//            }
        });
        waitCompleted(5000000);
    }

    private void queryByAdvanceFilterTest(ConnectorNode targetNode,TapConnectorContext connectionContext) throws Throwable{
        ConnectorFunctions connectorFunctions = targetNode.getConnectorFunctions();
        QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = connectorFunctions.getQueryByAdvanceFilterFunction();

        TapTable targetTable = targetNode.getConnectorContext().getTableMap().get(targetNode.getTable());

        Record record = Record.create()
                .builder("id", 111111)
                .builder("name", "gavin")
                .builder("text", "gavin test");
        RecordEventExecute recordEventExecute = RecordEventExecute.create(targetNode,connectionContext, this).builderRecord(record);


        recordEventExecute.insert();
        queryByAdvanceFilterFunction.query(
                targetNode.getConnectorContext(),
                TapAdvanceFilter.create().match(DataMap.create().kv("id", 111111).kv("name", "gavin").kv("text", "gavin test")),
                targetTable,
                filterResults -> {
                    $(() -> Assertions.assertNotNull(filterResults.getResults(), "Query results should be not null"));
                    $(() -> Assertions.assertTrue(objectIsEqual(filterResults.getResults(), Collections.singletonList(record)), "insert record not succeed."));
                });

        record.builder("name","Gavin pro").builder("text","Gavin pro max.");
        recordEventExecute.update();
        queryByAdvanceFilterFunction.query(
                targetNode.getConnectorContext(),
                TapAdvanceFilter.create().match(DataMap.create().kv("id", 111111).kv("name","Gavin pro").kv("text","Gavin pro max.")),
                targetTable,
                filterResults -> {
                    $(() -> Assertions.assertNotNull(filterResults.getResults(), "Query results should be not null"));
                    $(() -> Assertions.assertTrue(objectIsEqual(filterResults.getResults(), Collections.singletonList(record)), "update record not succeed."));
                });

        recordEventExecute.delete();
        queryByAdvanceFilterFunction.query(
                targetNode.getConnectorContext(),
                TapAdvanceFilter.create().match(DataMap.create().kv("id", 111111).kv("name", "gavin").kv("text", "gavin test")),
                targetTable,
                filterResults -> {
                    $(() -> Assertions.assertNotNull(filterResults.getResults(), "Query results should be not null"));
                    $(() -> Assertions.assertNotEquals(0, filterResults.getResults().size(), "delete record not succeed."));
                });
    }

    private void insertAfterInsertSomeKey(ConnectorNode targetNode,TapConnectorContext connectionContext) throws Throwable {
        Record[] records = Record.testStart(10);
        RecordEventExecute recordEventExecute = RecordEventExecute.create(targetNode,connectionContext, this)
                .builderRecord(records);

        recordEventExecute.createTable();
        WriteListResult<TapRecordEvent> insert = recordEventExecute.insert();
        for (Record record : records) {
            record.builder("name","Gavin pro").builder("text","Gavin pro max-modify");
        }

        final String insertPolicy = "dml_insert_policy";
        DataMap nodeConfig = targetNode.getConnectorContext().getNodeConfig();

        nodeConfig.kv(insertPolicy,"update_on_exists");
        WriteListResult<TapRecordEvent> insertAfter = recordEventExecute.insert();
        Assertions.assertNotEquals(
                insert.getInsertedCount(),
                insertAfter.getModifiedCount() + insertAfter.getInsertedCount(),
                insertPolicy+" - update_on_exists | The first time you insert "+
                        insert.getInsertedCount()+" record, the second time you insert "+
                        insert.getInsertedCount()+" records of the same primary key, the echo result is inserted "+
                        insertAfter.getInsertedCount()+", and the second time you update "+
                        insertAfter.getModifiedCount()+" record. The operation fails because of inconsistencies.");
        //@TODO Wran
        Assertions.assertNotEquals(
                insert.getInsertedCount(),
                insertAfter.getModifiedCount(),
                insertPolicy+" - update_on_exists | After inserting ten pieces of data, insert another record with the same primary key but different contents, and display the result. Insert " +
                        insertAfter.getInsertedCount() + ", insert another, and update " +
                        insertAfter.getModifiedCount() + ". Poor observability");


        nodeConfig.kv(insertPolicy,"ignore_on_exists");
        WriteListResult<TapRecordEvent> insertAfter2 = recordEventExecute.insert();
        Assertions.assertFalse(
                0 == insertAfter2.getModifiedCount() && 0 == insertAfter2.getInsertedCount(),
                insertPolicy+" - ignore_on_exists | In node config ,your choises is xxx,so the update count must be zero,but not zero now");
    }

    private void updateNotExistRecord(ConnectorNode targetNode,TapConnectorContext connectionContext) throws Throwable {
        Record[] records = Record.testStart(10);
        RecordEventExecute recordEventExecute = RecordEventExecute.create(targetNode,connectionContext, this)
                .builderRecord(records);

        recordEventExecute.createTable();
        WriteListResult<TapRecordEvent> insert = recordEventExecute.insert();
        for (Record record : records) {
            record.builder("name","Gavin pro").builder("text","Gavin pro max-modify");
        }
        WriteListResult<TapRecordEvent> insertAfter = recordEventExecute.insert();

        Assertions.assertNotEquals(
                insert.getInsertedCount(),
                insertAfter.getModifiedCount() + insertAfter.getInsertedCount(),
                "The first time you insert "+
                        insert.getInsertedCount()+" record, the second time you insert "+
                        insert.getInsertedCount()+" records of the same primary key, the echo result is inserted "+
                        insertAfter.getInsertedCount()+", and the second time you update "+
                        insertAfter.getModifiedCount()+" record. The operation fails because of inconsistencies.");
        String insertPolic = "";

        if ("".equals(insertPolic)) {
            //@TODO Wran
            Assertions.assertNotEquals(
                    insert.getInsertedCount(),
                    insertAfter.getModifiedCount(),
                    "After inserting ten pieces of data, insert another record with the same primary key but different contents, and display the result. Insert " +
                            insertAfter.getInsertedCount() + ", insert another, and update " +
                            insertAfter.getModifiedCount() + ". Poor observability");
        }else if("".equals(insertPolic)){
            Assertions.assertNotEquals(
                    0,
                    insertAfter.getModifiedCount(),
                    "In node config ,your choises is xxx,so the update count must be zero,but not zero now");
        }
    }

    private void deleteNotExistRecord(ConnectorNode targetNode,TapConnectorContext connectionContext) throws Throwable {
        Record[] records = Record.testStart(10);
        RecordEventExecute recordEventExecute = RecordEventExecute.create(targetNode,connectionContext, this)
                .builderRecord(records);

        recordEventExecute.createTable();
        WriteListResult<TapRecordEvent> insert = recordEventExecute.insert();
        for (Record record : records) {
            record.builder("name","Gavin pro").builder("text","Gavin pro max-modify");
        }
        WriteListResult<TapRecordEvent> insertAfter = recordEventExecute.insert();

        Assertions.assertNotEquals(
                insert.getInsertedCount(),
                insertAfter.getModifiedCount() + insertAfter.getInsertedCount(),
                "The first time you insert "+
                        insert.getInsertedCount()+" record, the second time you insert "+
                        insert.getInsertedCount()+" records of the same primary key, the echo result is inserted "+
                        insertAfter.getInsertedCount()+", and the second time you update "+
                        insertAfter.getModifiedCount()+" record. The operation fails because of inconsistencies.");
        String insertPolic = "";

        if ("".equals(insertPolic)) {
            //@TODO Wran
            Assertions.assertNotEquals(
                    insert.getInsertedCount(),
                    insertAfter.getModifiedCount(),
                    "After inserting ten pieces of data, insert another record with the same primary key but different contents, and display the result. Insert " +
                            insertAfter.getInsertedCount() + ", insert another, and update " +
                            insertAfter.getModifiedCount() + ". Poor observability");
        }else if("".equals(insertPolic)){
            Assertions.assertNotEquals(
                    0,
                    insertAfter.getModifiedCount(),
                    "In node config ,your choises is xxx,so the update count must be zero,but not zero now");
        }
    }

    public static List<SupportFunction> testFunctions() {
        return Arrays.asList(
                support(WriteRecordFunction.class, "WriteRecord is a must to verify batchRead and streamRead, please implement it in registerCapabilities method."),
                support(QueryByAdvanceFilterFunction.class, "QueryByAdvanceFilterFunction is a must for database which is schema free to sample some record to generate the field data types."),
                support(CreateTableV2Function.class,"Create table is must to verify ,please implement CreateTableV2Function in registerCapabilities method."),
                support(DropTableFunction.class,"Drop table is must to verify ,please implement DropTableFunction in registerCapabilities method.")
        );
    }

    private void initConnectorFunctions() {
        tddTargetNode = dataFlowWorker.getTargetNodeDriver(targetNodeId).getTargetNode();
        sourceNode = dataFlowWorker.getSourceNodeDriver(testSourceNodeId).getSourceNode();
    }

    public void tearDown() {
        super.tearDown();
    }
}
