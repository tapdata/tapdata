package io.tapdata.pdk.run.support;

import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.run.base.PDKBaseRun;
import io.tapdata.pdk.run.base.ReadStopException;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import io.tapdata.pdk.tdd.tests.v2.RecordEventExecute;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

@DisplayName("batchRead")
@TapGo(sort = 1)
public class ConnectionTestRun extends PDKBaseRun {
    @DisplayName("batchRead.afterInsert")
    @TapTestCase(sort = 1)
    @Test
    public void connectionTest() {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
//            List<TestItem> list = new ArrayList<>();
//            PDKTestBase.TestNode prepare = prepare(nodeInfo);
//            RecordEventExecute execute = prepare.recordEventExecute();
//            try {
//                Method testCase = super.getMethod("connectionTest");
//                execute.testCase(testCase);
//                ConnectorNode connectorNode = prepare.connectorNode();
//                TapConnector connector = connectorNode.getConnector();
//                TapConnectorContext context = connectorNode.getConnectorContext();
//                connector.connectionTest(context, consumer -> {
//                    if (Objects.nonNull(consumer)) {
//                        list.add(consumer);
//                    }
//                });
//            } catch (Throwable exception) {
//                if (!(exception instanceof ReadStopException)) {
//                    String message = exception.getMessage();
//                    System.out.printf("[DEBUG-%s] Error - %s, %s\n", super.testNodeId, message, list.isEmpty()?"Didn't get any results before this error.":"The following results were received before this error:\n"+toJson(list, JsonParser.ToJsonFeature.PrettyFormat,JsonParser.ToJsonFeature.WriteMapNullValue));
//                } else {
//                    String result = toJson(list, JsonParser.ToJsonFeature.PrettyFormat,JsonParser.ToJsonFeature.WriteMapNullValue);
//                    System.out.printf("[DEBUG-%s] Succeed - %s \n", super.testNodeId, result);
//                }
//            }
        });
    }
}
