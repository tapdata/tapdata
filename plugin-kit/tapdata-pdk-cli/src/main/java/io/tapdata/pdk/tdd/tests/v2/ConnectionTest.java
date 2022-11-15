package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVMapFactory;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;
import io.tapdata.pdk.apis.functions.connector.target.DropTableFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.cli.commands.TDDPrintf;
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
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Date;

@DisplayName("connectionTest.test")//连接测试，必测方法
@TapGo(sort = 5)
public class ConnectionTest extends PDKTestBase {

    //此方法不需要调用PDK数据源的init/stop方法， 直接调用connectionTest即可。 至少返回一个测试项即为成功。
    @DisplayName("connectionTest.testConnectionTest")//用例1， 返回恰当的测试结果
    @Test
    /**
     * Version， Connection， Login的TestItem项没有上报时， 输出警告。
     * 当实现BatchReadFunction的时候， Read没有上报时， 输出警告。
     * 当实现StreamReadFunction的时候， Read log没有上报时， 输出警告。
     * 当实现WriteRecordFunction的时候， Write没有上报时， 输出警告。
     * */
    void testConnectionTest(){
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            try {
                Method testCase = super.getMethod("insertWithQuery");
                TapConnectorContext connectorContext = prepare.connectorNode().getConnectorContext();
                ConnectorFunctions connectorFunctions = prepare.connectorNode().getConnectorFunctions();

                //Version， Connection， Login的TestItem项没有上报时， 输出警告。
                prepare.connectorNode().getConnector().connectionTest(connectorContext,
                        consumer->{
                            $(()->{
                                String item = consumer.getItem();
                                TapAssert.asserts(()->
                                    Assertions.assertTrue(
                                            TestItem.ITEM_CONNECTION.equals(item)||
                                                    TestItem.ITEM_VERSION.equals(item)||
                                                    TestItem.ITEM_LOGIN.equals(item),
                                            TapSummary.format("connectionTest.testConnectionTest.errorVCL")
                                    )
                                ).acceptAsWarn(this.get(),testCase,
                                        TapSummary.format("connectionTest.testConnectionTest.succeedVCL")
                                );

                                //当实现BatchReadFunction的时候， Read没有上报时， 输出警告。
                                BatchReadFunction batchReadFunction = connectorFunctions.getBatchReadFunction();
                                if (null!=batchReadFunction) {
                                    TapAssert.asserts(() ->
                                            Assertions.assertTrue(TestItem.ITEM_READ.equals(item),
                                                    TapSummary.format("connectionTest.testConnectionTest.errorBatchRead"))
                                    ).acceptAsWarn(
                                            this.get(), testCase,
                                            TapSummary.format("connectionTest.testConnectionTest.succeedBatchRead")
                                    );
                                }

                                //当实现StreamReadFunction的时候， Read log没有上报时， 输出警告。
                                StreamReadFunction streamReadFunction = connectorFunctions.getStreamReadFunction();
                                if (null!=streamReadFunction){
                                    TapAssert.asserts(()->
                                            Assertions.assertTrue(TestItem.ITEM_READ_LOG.equals(item),
                                                    TapSummary.format("connectionTest.testConnectionTest.errorStreamRead"))
                                    ).acceptAsWarn(
                                            this.get(),testCase,
                                            TapSummary.format("connectionTest.testConnectionTest.succeedStreamRead")
                                    );
                                }

                                //当实现WriteRecordFunction的时候， Write没有上报时， 输出警告。
                                WriteRecordFunction writeRecordFunction = connectorFunctions.getWriteRecordFunction();
                                if (null!=writeRecordFunction){
                                    TapAssert.asserts(()->
                                            Assertions.assertTrue(TestItem.ITEM_WRITE.equals(item),
                                                    TapSummary.format("connectionTest.testConnectionTest.errorWriteRecord"))
                                    ).acceptAsWarn(
                                            this.get(),testCase,
                                            TapSummary.format("connectionTest.testConnectionTest.succeedWriteRecord")
                                    );
                                }
                        });
                });

            }catch (Throwable e){
                throw new RuntimeException(e.getMessage());
            }

        });
    }

    private void initConnectorFunctions() {
        tddTargetNode = dataFlowWorker.getTargetNodeDriver(targetNodeId).getTargetNode();
        sourceNode = dataFlowWorker.getSourceNodeDriver(testSourceNodeId).getSourceNode();
    }

    public static List<SupportFunction> testFunctions() {
        List<SupportFunction> supportFunctions = Arrays.asList(

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
