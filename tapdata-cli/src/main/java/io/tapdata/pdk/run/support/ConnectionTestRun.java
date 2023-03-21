package io.tapdata.pdk.run.support;

import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.run.base.PDKBaseRun;
import io.tapdata.pdk.run.base.RunnerSummary;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.core.base.TestNode;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import io.tapdata.pdk.tdd.tests.basic.RecordEventExecute;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@DisplayName("connectionTestRun")
@TapGo(sort = 1)
public class ConnectionTestRun extends PDKBaseRun {
    @DisplayName("connectionTestRun.run")
    @TapTestCase(sort = 1)
    @Test
    public void connectionTest() throws NoSuchMethodException {
        Method testCase = super.getMethod("connectionTest");
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            try {
                super.connectorOnStart(prepare);
                execute.testCase(testCase);
                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnectorContext context = connectorNode.getConnectorContext();
                ConnectorFunctions functions = connectorNode.getConnectorFunctions();
                if (super.verifyFunctions(functions, testCase)) {
                    return;
                }
                TapConnector connector = connectorNode.getConnector();
                List<TestItem> events = new ArrayList<>();
                connector.connectionTest(context, consumer -> {
                    if (Objects.nonNull(consumer)) {
                        events.add(consumer);
                    }
                });
                super.runSucceed(testCase, RunnerSummary.format("formatValue", super.formatPatten(events)));
            } catch (Throwable exception) {
                super.runError(testCase, RunnerSummary.format("formatValue", exception.getMessage()));
            } finally {
                super.connectorOnStop(prepare);
            }
        });
    }
    public static List<SupportFunction> testFunctions() {
        return new ArrayList<>();
    }
}
