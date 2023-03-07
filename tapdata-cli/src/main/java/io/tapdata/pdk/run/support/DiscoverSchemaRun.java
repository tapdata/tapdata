package io.tapdata.pdk.run.support;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
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
import java.util.*;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 *
 * */
@DisplayName("discoverSchemaRun")
@TapGo(sort = 2)
public class DiscoverSchemaRun extends PDKBaseRun {
    @DisplayName("discoverSchemaRun.run")
    @TapTestCase(sort = 1)
    @Test
    public void discoverSchema() throws NoSuchMethodException {
        Method testCase = super.getMethod("discoverSchema");
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
                Map<String,Object> batchReadConfig = (Map<String,Object>)Optional.ofNullable(super.debugConfig.get("discover_schema")).orElse(new HashMap<>());
                final List<String> tableNames = (List<String>)Optional.ofNullable(batchReadConfig.get("tableNames")).orElse(new ArrayList<>());
                final int tableSize = (Integer)Optional.ofNullable(batchReadConfig.get("tableSize")).orElse(0);
                TapConnector connector = connectorNode.getConnector();
                List<TapTable> events = new ArrayList<>();
                connector.discoverSchema(context,tableNames,tableSize,consumer->{
                    if (Objects.nonNull(consumer) && !consumer.isEmpty()){
                        events.addAll(consumer);
                    }
                });
                super.runSucceed(testCase, RunnerSummary.format("formatValue",super.formatPatten(events)));
            } catch (Throwable exception) {
                super.runError(testCase, RunnerSummary.format("formatValue",exception.getMessage()));
            }finally {
                super.connectorOnStop(prepare);
            }
        });
    }
    public static List<SupportFunction> testFunctions() {
        return list(support(BatchReadFunction.class, RunnerSummary.format("jsFunctionInNeed","discover_schema")));
    }
}
