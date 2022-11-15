package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVMapFactory;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.QueryOperator;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.*;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.cli.commands.TapSummary;
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
import io.tapdata.pdk.tdd.tests.support.Record;
import io.tapdata.pdk.tdd.tests.support.TapAssert;
import io.tapdata.pdk.tdd.tests.target.DMLTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static io.tapdata.entity.simplify.TapSimplify.field;
import static io.tapdata.entity.simplify.TapSimplify.table;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Long;


@DisplayName("Test.WriteRecordWithQueryTest")
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

    TapTable targetTable = table(testTableId)
            .add(field("id", JAVA_Long).isPrimaryKey(true).primaryKeyPos(1))
            .add(field("name", "STRING"))
            .add(field("text", "STRING"));
    @Test
    @DisplayName("Test.WriteRecordWithQueryTest.case.sourceTest")
    void sourceTest() throws Throwable {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = prepare(nodeInfo);
            //showCapabilities(connectorNode);
            try{
                Method method = this.getMethod("sourceTest");

                PDKInvocationMonitor.invoke(prepare.connectorNode(), PDKMethod.INIT,prepare.connectorNode()::connectorInit,"Init PDK","TEST mongodb");
                queryByAdvanceFilterTest(prepare.connectorNode(),prepare.recordEventExecute().testCase(method));
//
//                insertAfterInsertSomeKey(dataFlowWorker.getTargetNodeDriver(testTargetNodeId).getTargetNode(),this.getMethod(""),connectionContext);
//
//                updateNotExistRecord(dataFlowWorker.getTargetNodeDriver(testTargetNodeId).getTargetNode(),this.getMethod(""),connectionContext);
//
//                deleteNotExistRecord(dataFlowWorker.getTargetNodeDriver(testTargetNodeId).getTargetNode(),this.getMethod(""),connectionContext);

            }catch (Throwable e){
                throw new RuntimeException(e);
            }finally {
                prepare.recordEventExecute().dropTable();
                if (null != prepare.connectorNode()){
                    PDKInvocationMonitor.invoke(prepare.connectorNode(), PDKMethod.STOP,prepare.connectorNode()::connectorStop,"Stop PDK","TEST mongodb");
                    PDKIntegration.releaseAssociateId("releaseAssociateId");
                }
            }
        });
        //waitCompleted(5000000);
    }

    @Test
    @DisplayName("Test.WriteRecordWithQueryTest.case.sourceTest2")
    void sourceTest2() throws Throwable {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = prepare(nodeInfo);
            //showCapabilities(connectorNode);
            try{
                Method method = this.getMethod("sourceTest2");
                PDKInvocationMonitor.invoke(prepare.connectorNode(), PDKMethod.INIT,prepare.connectorNode()::connectorInit,"Init PDK","TEST mongodb");
                insertAfterInsertSomeKey(prepare.connectorNode(),prepare.recordEventExecute().testCase(method));
            }catch (Throwable e){
                throw new RuntimeException(e);
            }finally {
                prepare.recordEventExecute().dropTable();
                if (null != prepare.connectorNode()){
                    PDKInvocationMonitor.invoke(prepare.connectorNode(), PDKMethod.STOP,prepare.connectorNode()::connectorStop,"Stop PDK","TEST mongodb");
                    PDKIntegration.releaseAssociateId("releaseAssociateId");
                }
            }
        });
        //waitCompleted(5000000);
    }

    private void queryByAdvanceFilterTest(ConnectorNode targetNode,RecordEventExecute recordEventExecute) throws Throwable{
        ConnectorFunctions connectorFunctions = targetNode.getConnectorFunctions();
        QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = connectorFunctions.getQueryByAdvanceFilterFunction();

        Record record = Record.create()
                .builder("id", 111111)
                .builder("name", "gavin")
                .builder("text", "gavin test");
        recordEventExecute.builderRecord(record);
        Method testCase = recordEventExecute.testCase();

        //插入一条记录，并获取查询结果对比插入前后是否一致
        recordEventExecute.insert();
        queryByAdvanceFilterFunction.query(
                targetNode.getConnectorContext(),
                TapAdvanceFilter.create().op(QueryOperator.lte("id", 111111)).op(QueryOperator.gte("id", 111111)),
                targetTable,
                filterResults -> $(() -> {
                    TapAssert.asserts(
                            ()->Assertions.assertNotNull(filterResults, TapSummary.format("WriteRecordWithQueryTest.sourceTest.insert.error.null"))//
                    ).acceptAsError(this.getClass(),testCase,TapSummary.format("WriteRecordWithQueryTest.sourceTest.insert.succeed.notNull"));//

                    TapAssert.asserts(
                            ()->Assertions.assertNotNull(filterResults.getResults(), TapSummary.format("WriteRecordWithQueryTest.sourceTest.insert.error.nullResult"))//
                    ).acceptAsError(this.getClass(),testCase,TapSummary.format("WriteRecordWithQueryTest.sourceTest.insert.succeed.notNullResult"));//

                    TapAssert.asserts(
                            ()-> Assertions.assertTrue(objectIsEqual(
                                filterResults.getResults(),
                                Collections.singletonList(record)),
                                TapSummary.format("WriteRecordWithQueryTest.sourceTest.insert.error.notEquals"))//
                    ).acceptAsWarn(this.getClass(),testCase,TapSummary.format("WriteRecordWithQueryTest.sourceTest.insert.succeed.equals"));//
                })
        );

        //修改插入的记录，并对比插入前后的结果是否一致
        record.builder("name","Gavin pro").builder("text","Gavin pro max.");
        recordEventExecute.update();
        queryByAdvanceFilterFunction.query(
                targetNode.getConnectorContext(),
                TapAdvanceFilter.create().match(DataMap.create().kv("id", 111111).kv("name","Gavin pro").kv("text","Gavin pro max.")),
                targetTable,
                filterResults -> {
                    TapAssert.asserts(
                            ()->Assertions.assertNotNull(filterResults, TapSummary.format("WriteRecordWithQueryTest.sourceTest.update.error.null"))
                    ).acceptAsError(this.getClass(),testCase,TapSummary.format("WriteRecordWithQueryTest.sourceTest.update.succeed.notNull"));

                    TapAssert.asserts(
                            ()->Assertions.assertNotNull(filterResults.getResults(), TapSummary.format("WriteRecordWithQueryTest.sourceTest.update.error.nullResult"))
                    ).acceptAsError(this.getClass(),testCase,TapSummary.format("WriteRecordWithQueryTest.sourceTest.update.succeed.notNullResult"));

                    TapAssert.asserts(
                            ()-> Assertions.assertTrue(objectIsEqual(
                                    filterResults.getResults(),
                                    Collections.singletonList(record)),
                                    TapSummary.format("WriteRecordWithQueryTest.sourceTest.update.error.notEquals"))
                    ).acceptAsWarn(this.getClass(),testCase,TapSummary.format("WriteRecordWithQueryTest.sourceTest.update.succeed.equals"));
                });

        //删除插入的记录，并检查删除是否成功
        recordEventExecute.delete();
        queryByAdvanceFilterFunction.query(
                targetNode.getConnectorContext(),
                TapAdvanceFilter.create().match(DataMap.create().kv("id", 111111).kv("name", "gavin").kv("text", "gavin test")),
                targetTable,
                filterResults -> {
                    TapAssert.asserts(
                            ()->Assertions.assertNotNull(filterResults, TapSummary.format("WriteRecordWithQueryTest.sourceTest.delete.error.null"))
                    ).acceptAsError(this.getClass(),testCase,TapSummary.format("WriteRecordWithQueryTest.sourceTest.delete.succeed.notNull"));

                    TapAssert.asserts(
                            ()->Assertions.assertNull(filterResults.getResults(), TapSummary.format("WriteRecordWithQueryTest.sourceTest.delete.error.notNullResult"))
                    ).acceptAsError(this.getClass(),testCase,TapSummary.format("WriteRecordWithQueryTest.sourceTest.delete.succeed.nullResult"));
                });
    }

    private void insertAfterInsertSomeKey(ConnectorNode targetNode,RecordEventExecute recordEventExecute) throws Throwable {
        Record[] records = Record.testStart(1);
        recordEventExecute.builderRecord(records);
        Method testCase = recordEventExecute.testCase();

        //recordEventExecute.createTable();
        WriteListResult<TapRecordEvent> insert = recordEventExecute.insert();

        TapAssert.asserts(()->{Assertions.assertTrue(false,"This is an warn Case");})
                .acceptAsWarn(this.getClass(),testCase,"This is an warn case.");

        TapAssert.asserts(()->{Assertions.assertTrue(false,"This is error Case");})
                .acceptAsError(this.getClass(),testCase,"This is succeed case.");

        for (Record record : records) {
            record.builder("name","Gavin pro").builder("text","Gavin pro max-modify");
        }

        final String insertPolicy = "dml_insert_policy";
        DataMap nodeConfig = targetNode.getConnectorContext().getNodeConfig();

        nodeConfig.kv(insertPolicy,"update_on_exists");
        WriteListResult<TapRecordEvent> insertAfter = recordEventExecute.insert();

        //插入已存在数据时， 在策略为update_on_exists时， 应该返回给引擎有插入成功的计数统计。
        TapAssert.asserts(
            ()->Assertions.assertNotEquals(
                    insert.getInsertedCount(),
                    insertAfter.getModifiedCount() + insertAfter.getInsertedCount(),
                    insertPolicy+" - update_on_exists | The first time you insert "+
                            insert.getInsertedCount()+" record, the second time you insert "+
                            insert.getInsertedCount()+" records of the same primary key, the echo result is inserted "+
                            insertAfter.getInsertedCount()+", and the second time you update "+
                            insertAfter.getModifiedCount()+" record. The operation fails because of inconsistencies.")
        ).acceptAsError(
                this.getClass(),
                testCase,
                "As update_on_exists policy,you insert "
                        +insert.getInsertedCount()
                        +" records, and succeed "
                        +(insertAfter.getModifiedCount() + insertAfter.getInsertedCount())
                        + " records, including modifiedCount and insertedCount.");

        TapAssert.asserts(()->Assertions.assertNotEquals(
                insert.getInsertedCount(),
                insertAfter.getModifiedCount(),
                insertPolicy + " - update_on_exists | After inserting ten pieces of data, insert another record with the same primary key but different contents, and display the result. Insert " +
                        insertAfter.getInsertedCount() + ", insert another, and update " +
                        insertAfter.getModifiedCount() + ". Poor observability")).acceptAsWarn(this.getClass(),testCase,"");

        //@TODO Wran
        Assertions.assertNotEquals(
                insert.getInsertedCount(),
                insertAfter.getModifiedCount(),
                insertPolicy + " - update_on_exists | After inserting ten pieces of data, insert another record with the same primary key but different contents, and display the result. Insert " +
                        insertAfter.getInsertedCount() + ", insert another, and update " +
                        insertAfter.getModifiedCount() + ". Poor observability");


        nodeConfig.kv(insertPolicy,"ignore_on_exists");
        WriteListResult<TapRecordEvent> insertAfter2 = recordEventExecute.insert();
        Assertions.assertFalse(
                0 == insertAfter2.getModifiedCount() && 0 == insertAfter2.getInsertedCount(),
                insertPolicy+" - ignore_on_exists | In node config ,your choises is xxx,so the update count must be zero,but not zero now");
    }

    private void updateNotExistRecord(ConnectorNode targetNode,RecordEventExecute recordEventExecute) throws Throwable {
        Record[] records = Record.testStart(10);
        recordEventExecute.builderRecord(records);
        Method testCase = recordEventExecute.testCase();
        //recordEventExecute.createTable();
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

    private void deleteNotExistRecord(ConnectorNode targetNode,RecordEventExecute recordEventExecute) throws Throwable {
        Record[] records = Record.testStart(10);
        recordEventExecute.builderRecord(records);
        Method testCase = recordEventExecute.testCase();

        //recordEventExecute.createTable();
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
//                support(CreateTableFunction.class,"Create table is must to verify ,please implement CreateTableFunction in registerCapabilities method."),
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

    @Override
    public Class<? extends PDKTestBase> get() {
        return this.getClass();
    }

    private TestNode prepare(TapNodeInfo nodeInfo){
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
        String dagId = UUID.randomUUID().toString();

        testTableId = "test";UUID.randomUUID().toString();
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
        KVMap<TapTable> kvMap = InstanceFactory.instance(KVMapFactory.class).getCacheMap(dagId, TapTable.class);
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
        connectorNode.getConnectorContext().setNodeConfig(new DataMap());
//        try {
//            Class cla = connectorNode.getClass();
//            Field connectorContext = cla.getDeclaredField("connectorContext");
//            connectorContext.setAccessible(true);
//            connectorContext.set(connectorNode,connectionContext);
//            connectorNode.getConnectorContext().setNodeConfig(new DataMap());
//        }catch (Exception e){
//        }
        RecordEventExecute recordEventExecute = RecordEventExecute.create(connectorNode, this);
        return new TestNode( connectorNode, recordEventExecute);
    }
}
