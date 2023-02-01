package io.tapdata.pdk.debug.support;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.debug.base.PDKBaseDebug;
import io.tapdata.pdk.debug.base.ReadStopException;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import io.tapdata.pdk.tdd.tests.v2.RecordEventExecute;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

@DisplayName("batchRead")
@TapGo(sort = 1)
public class ConnectionTestDebug extends PDKBaseDebug {
    @DisplayName("batchRead.afterInsert")
    @TapTestCase(sort = 1)
    @Test
    public void connectionTest(){
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            List<TestItem> list = new ArrayList<>();
            PDKTestBase.TestNode prepare = prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            try {
                Method testCase = super.getMethod("connectionTest");
                super.connectorOnStart(prepare);
                execute.testCase(testCase);
                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnector connector = connectorNode.getConnector();
                TapConnectorContext context = connectorNode.getConnectorContext();
                connector.connectionTest(context, consumer->{
                    if (Objects.nonNull(consumer)){
                        list.add(consumer);
                    }
                });
            } catch (Throwable exception) {
                super.connectorOnStop(prepare);
                if (!(exception instanceof ReadStopException)){
                    String message = exception.getMessage();
                    System.out.println(message);
                }else {
                    String result = toJson(list);
                    System.out.println(result);
                }
            }
        });
    }
}
