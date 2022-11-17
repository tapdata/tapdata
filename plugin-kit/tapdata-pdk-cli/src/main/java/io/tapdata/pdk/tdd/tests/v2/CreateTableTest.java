package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.*;
import io.tapdata.pdk.cli.commands.TapSummary;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.support.TapAssert;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static io.tapdata.entity.simplify.TapSimplify.list;

@DisplayName("createTableTest.test")//CreateTableFunction/CreateTableV2Function建表
@TapGo(sort = 9)
public class CreateTableTest extends PDKTestBase {

    @DisplayName("createTableV2")//用例1，CreateTableFunction已过期， 应使用CreateTableV2Function
    @TapTestCase(sort = 1)
    @Test
    /**
     * CreateTableFunction和CreateTableV2Function的差距是，
     * V2版本会返回CreateTableOptions对象，
     * 以下测试用例这两个方法都需要测试，
     * 如果同时实现了两个方法只需要测试CreateTableV2Function。
     * 检查如果只实现了CreateTableFunction，没有实现CreateTableV2Function时，报出警告， 推荐使用CreateTableV2Function方法来实现建表
     * */
    void createTableV2(){
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            try {
                PDKInvocationMonitor.invoke(prepare.connectorNode(),
                        PDKMethod.INIT,
                        prepare.connectorNode()::connectorInit,
                        "Init PDK","TEST mongodb"
                );
                Method testCase = super.getMethod("createTableV2");
                prepare.recordEventExecute().testCase(testCase);

                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnectorContext connectorContext = connectorNode.getConnectorContext();
                ConnectorFunctions functions = connectorNode.getConnectorFunctions();

                if (super.verifyFunctions(functions,testCase)){
                    return;
                }

                CreateTableFunction createTable = functions.getCreateTableFunction();
                TapAssert.asserts(()->
                        Assertions.assertNotNull(createTable, TapSummary.format("createTable.null"))
                ).acceptAsWarn(testCase,TapSummary.format("createTable.notNull"));


                CreateTableV2Function createTableV2 = functions.getCreateTableV2Function();
                TapAssert.asserts(()->
                        Assertions.assertNotNull(createTableV2, TapSummary.format("createTable.v2Null"))
                ).acceptAsWarn(testCase,TapSummary.format("createTable.v2NotNull"));

                boolean isV1 = null != createTable;
                boolean isV2 = null != createTableV2;

                if (!isV1 && !isV2){
                    return;
                }
                String tableId = UUID.randomUUID().toString();
                targetTable.setId(tableId);
                TapCreateTableEvent tableEvent = new TapCreateTableEvent().table(targetTable);
                //如果同时实现了两个方法只需要测试CreateTableV2Function。(V2优先)
                if (isV2){
                    CreateTableOptions table = createTableV2.createTable(
                            connectorContext,
                            tableEvent
                    );
                    this.verifyTableIsCreated("CreateTableV2Function",prepare);
                    return;
                }
                //检查如果只实现了CreateTableFunction，没有实现CreateTableV2Function时，报出警告，
                //推荐使用CreateTableV2Function方法来实现建表
                createTable.createTable(connectorContext, tableEvent);
                if (this.verifyTableIsCreated("CreateTableFunction", prepare)) {
                    prepare.recordEventExecute().dropTable();
                }
            }catch (Throwable e) {
                throw new RuntimeException(e);
            }finally {
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

    boolean verifyTableIsCreated(String createMethod,TestNode prepare) throws Throwable {
        ConnectorNode connectorNode = prepare.connectorNode();
        TapConnector connector = connectorNode.getConnector();
        TapConnectorContext connectorContext = connectorNode.getConnectorContext();
        String tableIdTarget = targetTable.getId();
        Method testBase = prepare.recordEventExecute().testCase();
        List<TapTable> consumer = new ArrayList<>();
        connector.discoverSchema(connectorContext,list(tableIdTarget),1000,con->{
            if(null!=con) consumer.addAll(con);
        });
        TapAssert.asserts(()->{
            Assertions.assertFalse(consumer.isEmpty(), TapSummary.format("verifyTableIsCreated.error", createMethod, tableIdTarget));
        }).acceptAsError(testBase,TapSummary.format("verifyTableIsCreated.succeed",createMethod,tableIdTarget));
        return !consumer.isEmpty();
    }


    @DisplayName("allTapType")//用例2， 使用TapType全类型11个类型推演建表测试
    @TapTestCase(sort = 2)
    @Test
    void allTapType(){
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            PDKTestBase.TestNode prepare = this.prepare(nodeInfo);
            try {
                PDKInvocationMonitor.invoke(prepare.connectorNode(),
                        PDKMethod.INIT,
                        prepare.connectorNode()::connectorInit,
                        "Init PDK","TEST mongodb"
                );
                Method testCase = super.getMethod("allTapType");
                prepare.recordEventExecute().testCase(testCase);

                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnector connector = connectorNode.getConnector();
                TapConnectorContext connectorContext = connectorNode.getConnectorContext();

            }catch (Throwable e) {
                throw new RuntimeException(e);
            }finally {
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


    @DisplayName("addIndex")//用例3， 建表时增加索引信息进行测试
    @TapTestCase(sort = 3)
    @Test
    void addIndex(){
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            PDKTestBase.TestNode prepare = this.prepare(nodeInfo);
            try {
                PDKInvocationMonitor.invoke(prepare.connectorNode(),
                        PDKMethod.INIT,
                        prepare.connectorNode()::connectorInit,
                        "Init PDK","TEST mongodb"
                );
                Method testCase = super.getMethod("addIndex");
                prepare.recordEventExecute().testCase(testCase);

                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnector connector = connectorNode.getConnector();
                TapConnectorContext connectorContext = connectorNode.getConnectorContext();

            }catch (Throwable e) {
                throw new RuntimeException(e);
            }finally {
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


    @DisplayName("tableIfExist")//用例4， 建表时表是否存在的测试
    @TapTestCase(sort = 4)
    @Test
    void tableIfExist(){
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            PDKTestBase.TestNode prepare = this.prepare(nodeInfo);
            try {
                PDKInvocationMonitor.invoke(prepare.connectorNode(),
                        PDKMethod.INIT,
                        prepare.connectorNode()::connectorInit,
                        "Init PDK","TEST mongodb"
                );
                Method testCase = super.getMethod("tableIfExist");
                prepare.recordEventExecute().testCase(testCase);

                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnector connector = connectorNode.getConnector();
                TapConnectorContext connectorContext = connectorNode.getConnectorContext();

            }catch (Throwable e) {
                throw new RuntimeException(e);
            }finally {
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

    public static List<SupportFunction> testFunctions() {
        return list(
                support(DropTableFunction.class,""),
                supportAny(list(WriteRecordFunction.class,CreateTableFunction.class,CreateTableV2Function.class),"")
        );
    }
}
