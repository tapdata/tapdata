package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.core.base.TestNode;
import io.tapdata.pdk.tdd.tests.support.LangUtil;
import io.tapdata.pdk.tdd.tests.support.TapAssert;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.list;

@DisplayName("connectionTest.test")//连接测试，必测方法,
@TapGo(tag = "V2", sort = 0, block = true)
public class ConnectionTest extends PDKTestBase {
    {
        if (PDKTestBase.testRunning) {
            System.out.println(LangUtil.format("connectionTest.wait"));
        }
    }
    //此方法不需要调用PDK数据源的init/stop方法， 直接调用connectionTest即可。 至少返回一个测试项即为成功。
    @DisplayName("connectionTest.testConnectionTest")//用例1， 返回恰当的测试结果
    @Test
    @TapTestCase(sort = 1)
    /**
     * Version， Connection， Login的TestItem项没有上报时， 输出警告。
     * 当实现BatchReadFunction的时候， Read没有上报时， 输出警告。
     * 当实现StreamReadFunction的时候， Read log没有上报时， 输出警告。
     * 当实现WriteRecordFunction的时候， Write没有上报时， 输出警告。
     * */
    void testConnectionTest() {
        System.out.println(LangUtil.format("testConnectionTest.wait"));
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            try {
                Method testCase = super.getMethod("testConnectionTest");
                TapConnectorContext connectorContext = prepare.connectorNode().getConnectorContext();
                ConnectorFunctions connectorFunctions = prepare.connectorNode().getConnectorFunctions();
                //Version， Connection， Login的TestItem项没有上报时， 输出警告。
                Map<String, TestItem> testItemMap = new HashMap<>();
                prepare.connectorNode().getConnector().connectionTest(connectorContext, consumer -> {
                    if (null != consumer) testItemMap.put(consumer.getItem(), consumer);
                });
                //String item = consumer.getItem();
                TapAssert.asserts(() ->
                        Assertions.assertTrue(
                                testItemMap.containsKey(TestItem.ITEM_CONNECTION) ||
                                        testItemMap.containsKey(TestItem.ITEM_VERSION) ||
                                        testItemMap.containsKey(TestItem.ITEM_LOGIN),
                                LangUtil.format("connectionTest.testConnectionTest.errorVCL")
                        )
                ).acceptAsWarn(testCase,
                        LangUtil.format("connectionTest.testConnectionTest.succeedVCL")
                );
                //当实现BatchReadFunction的时候， Read没有上报时， 输出警告。
                BatchReadFunction batchReadFunction = connectorFunctions.getBatchReadFunction();
                if (null != batchReadFunction) {
                    TapAssert.asserts(() ->
                            Assertions.assertTrue(testItemMap.containsKey(TestItem.ITEM_READ),
                                    LangUtil.format("connectionTest.testConnectionTest.errorBatchRead"))
                    ).acceptAsWarn(testCase,
                            LangUtil.format("connectionTest.testConnectionTest.succeedBatchRead")
                    );
                }
                //当实现StreamReadFunction的时候， Read log没有上报时， 输出警告。
                StreamReadFunction streamReadFunction = connectorFunctions.getStreamReadFunction();
                if (null != streamReadFunction) {
                    TapAssert.asserts(() ->
                            Assertions.assertTrue(testItemMap.containsKey(TestItem.ITEM_READ_LOG),
                                    LangUtil.format("connectionTest.testConnectionTest.errorStreamRead"))
                    ).acceptAsWarn(testCase,
                            LangUtil.format("connectionTest.testConnectionTest.succeedStreamRead")
                    );
                }

                //当实现WriteRecordFunction的时候， Write没有上报时， 输出警告。
                WriteRecordFunction writeRecordFunction = connectorFunctions.getWriteRecordFunction();
                if (null != writeRecordFunction) {
                    TapAssert.asserts(() ->
                            Assertions.assertTrue(testItemMap.containsKey(TestItem.ITEM_WRITE),
                                    LangUtil.format("connectionTest.testConnectionTest.errorWriteRecord"))
                    ).acceptAsWarn(testCase,
                            LangUtil.format("connectionTest.testConnectionTest.succeedWriteRecord")
                    );
                }
            } catch (Throwable e) {
                throw new RuntimeException(e.getMessage());
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return list();
    }
}
