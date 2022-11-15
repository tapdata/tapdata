package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVMapFactory;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.DropTableFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.cli.commands.TapSummary;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.workflow.engine.DataFlowWorker;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.support.Record;
import io.tapdata.pdk.tdd.tests.support.TapAssert;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static io.tapdata.entity.simplify.TapSimplify.field;
import static io.tapdata.entity.simplify.TapSimplify.table;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.*;

@DisplayName("test.queryByFilterTest")//QueryByFilterFunction基于匹配字段查询（依赖WriteRecordFunction）
@TapGo(sort = 4)
public class QueryByFilterTest extends PDKTestBase {
    private static final String TAG = QueryByFilterTest.class.getSimpleName();

    //private void queryByFilter(TapConnectorContext connectorContext, List<TapFilter> filters, TapTable tapTable, Consumer<List<FilterResult>> listConsumer)
    @Test
    @DisplayName("test.queryByFilterTest.insertWithQuery")//用例1，插入数据能正常查询并进行值比对
    /**
     * 使用WriteRecordFunction插入一条全类型（覆盖TapType的11中类型数据）数据，通过主键作为匹配参数， 查询出该条数据，
     * 再进行精确和模糊值匹配， 只要能查出来数据就算是正确。
     * 如果值只能通过模糊匹配成功， 报警告指出是靠模糊匹配成功的，
     * 如果连模糊匹配也匹配不上， 也报警告（值对与不对很多时候不好判定）。
     * */
    void insertWithQuery() throws Throwable{
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            try {
                PDKInvocationMonitor.invoke(prepare.connectorNode(),
                        PDKMethod.INIT, prepare.connectorNode()::connectorInit,
                        "Init PDK","TEST mongodb");
                Record[] records = Record.testRecordWithTapTable(targetTable, 1);
                RecordEventExecute recordEventExecute = prepare.recordEventExecute();
                Method testCase = super.getMethod("insertWithQuery");
                recordEventExecute.testCase(testCase);
                recordEventExecute.builderRecord(records);
                WriteListResult<TapRecordEvent> insert = null;
                try {
                    //使用WriteRecordFunction插入一条全类型（覆盖TapType的11中类型数据）数据，通过主键作为匹配参数， 查询出该条数据，
                    insert = recordEventExecute.insert();
                    WriteListResult<TapRecordEvent> finalInsert = insert;
                    TapAssert.asserts(()->
                            Assertions.assertTrue(null != finalInsert && finalInsert.getInsertedCount() == records.length,
                                    TapSummary.format(""))
                    ).acceptAsError(this.get(),testCase,
                                    TapSummary.format(""));

                    ConnectorFunctions connectorFunctions = prepare.connectorNode().getConnectorFunctions();
                    QueryByFilterFunction queryByFilterFunction = connectorFunctions.getQueryByFilterFunction();
                    //通过主键作为匹配参数， 查询出该条数据，
                    //@TODO filters
                    List<TapFilter> filters = null;
                    queryByFilterFunction.query(
                            prepare.connectorNode().getConnectorContext(),
                            filters,
                            targetTable,
                            filterResults -> $(() -> {
                                TapAssert.asserts(()->{

                                }).acceptAsError(
                                        this.get(),testCase,
                                        TapSummary.format("")
                                );
                            }));

                    //再进行模糊值匹配， 只要能查出来数据就算是正确。如果值只能通过模糊匹配成功， 报警告指出是靠模糊匹配成功的，
                    //如果连模糊匹配也匹配不上， 也报警告（值对与不对很多时候不好判定）。
                    //@TODO filters
                    filters = null;
                    queryByFilterFunction.query(
                            prepare.connectorNode().getConnectorContext(),
                            filters,
                            targetTable,
                            filterResults -> $(() -> {
                                TapAssert.asserts(()->{

                                }).acceptAsError(
                                        this.get(),testCase,
                                        TapSummary.format("")
                                );
                            }));

                    //再进行精确， 只要能查出来数据就算是正确。
                    //@TODO filters
                    filters = null;
                    queryByFilterFunction.query(
                            prepare.connectorNode().getConnectorContext(),
                            filters,
                            targetTable,
                            filterResults -> $(() -> {
                                TapAssert.asserts(()->{

                                }).acceptAsError(
                                        this.get(),testCase,
                                        TapSummary.format("")
                                );
                            }));
                }catch (Throwable e){
                    TapAssert.asserts(()->
                            Assertions.fail(TapSummary.format(""))
                    ).acceptAsError(
                            this.get(),testCase,
                            TapSummary.format("")
                    );
                }
            }catch (Exception e){
                throw new RuntimeException(e);
            }finally {
                prepare.recordEventExecute().dropTable();
                if (null != prepare.connectorNode()){
                    PDKInvocationMonitor.invoke(prepare.connectorNode(),
                            PDKMethod.STOP,
                            prepare.connectorNode()::connectorStop,
                            "Stop PDK",
                            "TEST mongodb"
                    );
                    PDKIntegration.releaseAssociateId("releaseAssociateId");
                }
            }
        });
    }


    @Test
    @DisplayName("test.queryByFilterTest.queryWithLotTapFilter")//用例2，查询数据时，指定多个TapFilter，需要返回多个FilterResult，做一一对应
    /**
     * 使用WriteRecordFunction插入一条全类型（覆盖TapType的11中类型数据）数据，通过主键作为匹配参数， 查询出该条数据，
     * 再进行精确和模糊值匹配， 只要能查出来数据就算是正确。
     * 如果值只能通过模糊匹配成功， 报警告指出是靠模糊匹配成功的，
     * 如果连模糊匹配也匹配不上， 也报警告（值对与不对很多时候不好判定）。
     * */
    void queryWithLotTapFilter() throws Throwable{
        consumeQualifiedTapNodeInfo(nodeInfo -> {

        });
    }

    private void initConnectorFunctions() {
        tddTargetNode = dataFlowWorker.getTargetNodeDriver(targetNodeId).getTargetNode();
        sourceNode = dataFlowWorker.getSourceNodeDriver(testSourceNodeId).getSourceNode();
    }

    public static List<SupportFunction> testFunctions() {
        List<SupportFunction> supportFunctions = Arrays.asList(
                support(WriteRecordFunction.class, "WriteRecord is a must to verify batchRead and streamRead, please implement it in registerCapabilities method."),
                support(QueryByFilterFunction.class,"QueryByFilterFunction is must to verify ,please implement QueryByFilterFunction in registerCapabilities method."),
                support(DropTableFunction.class, "Drop table is must to verify ,please implement DropTableFunction in registerCapabilities method.")
                //support(QueryByAdvanceFilterFunction.class, "QueryByAdvanceFilterFunction is a must for database which is schema free to sample some record to generate the field data types.")
                //support(DropTableFunction.class, "DropTable is needed for TDD to drop the table created by tests, please implement it in registerCapabilities method.")
        );
        return supportFunctions;
    }

    public void tearDown() {
        super.tearDown();
    }
    @Override
    public Class<? extends PDKTestBase> get() {
        return this.getClass();
    }
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
            .add(field("TYPE_ARRAY", JAVA_Array))
            .add(field("TYPE_BINARY", JAVA_Binary))
            .add(field("TYPE_BOOLEAN", JAVA_Boolean))
            .add(field("TYPE_DATE", JAVA_Date))
            .add(field("TYPE_DATETIME", JAVA_Date))
            .add(field("TYPE_MAP", JAVA_Map))
            .add(field("TYPE_NUMBER_Long", JAVA_Long))
            .add(field("TYPE_NUMBER_INTEGER", JAVA_Integer))
            .add(field("TYPE_NUMBER_BigDecimal", JAVA_BigDecimal))
            .add(field("TYPE_NUMBER_Float", JAVA_Float))
            .add(field("TYPE_NUMBER_Double", JAVA_Double))
            .add(field("TYPE_STRING", JAVA_String))
            .add(field("TYPE_TIME", JAVA_Date))
            .add(field("TYPE_YEAR", JAVA_Date));


    private TestNode prepare(TapNodeInfo nodeInfo){
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
