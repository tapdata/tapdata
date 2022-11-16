package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.schema.TapField;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.cli.commands.TapSummary;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.tests.support.TapAssert;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

@DisplayName("CreateTableTest.test")//CreateTableFunction/CreateTableV2Function建表
@TapGo(sort = 9)
public class CreateTableTest extends PDKTestBase {

    @DisplayName("createTableV2")//用例1，CreateTableFunction已过期， 应使用CreateTableV2Function
    @TapTestCase(sort = 1)
    @Test
    void createTableV2(){
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            PDKTestBase.TestNode prepare = this.prepare(nodeInfo);
            try {
                PDKInvocationMonitor.invoke(prepare.connectorNode(),
                        PDKMethod.INIT,
                        prepare.connectorNode()::connectorInit,
                        "Init PDK","TEST mongodb"
                );
                Method testCase = super.getMethod("createTableV2");
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
}
