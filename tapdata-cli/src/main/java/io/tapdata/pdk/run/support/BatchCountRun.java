package io.tapdata.pdk.run.support;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.run.base.PDKBaseRun;
import io.tapdata.pdk.run.base.RunClassMap;
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

@DisplayName("batchCountRun")
@TapGo(sort = 4)
public class BatchCountRun extends PDKBaseRun {
    private static final String jsName = RunClassMap.BATCH_COUNT_RUN.jsName(0);

    @DisplayName("batchCountRun.run")
    @TapTestCase(sort = 1)
    @Test
    public void batchCount() throws NoSuchMethodException {
        Method testCase = super.getMethod("batchCount");
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            try {
                super.connectorOnStart(prepare);
                execute.testCase(testCase);

                ConnectorNode connectorNode = prepare.connectorNode();
                ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
                BatchCountFunction batchCountFunction = connectorFunctions.getBatchCountFunction();
                if (Objects.nonNull(batchCountFunction)) {
                    Map<String, Object> batchCountConfig = (Map<String, Object>) Optional.ofNullable(super.debugConfig.get(BatchCountRun.jsName)).orElse(new HashMap<>());
                    Object tableName = batchCountConfig.get("tableName");
                    if (Objects.isNull(tableName)) {
                        super.runError(testCase, RunnerSummary.format("batchCountRun.noTable"));
                        return;
                    }
                    TapTable table = new TapTable(String.valueOf(tableName), String.valueOf(tableName));
                    long count = batchCountFunction.count(connectorNode.getConnectorContext(), table);
                    super.runSucceed(testCase, RunnerSummary.format("batchCountRun.succeed", tableName, count));
                } else {
                    //Error cannot support batch count function
                    super.runError(testCase, RunnerSummary.format("batchCountRun.noFunction"));
                }
            } catch (Throwable exception) {
                super.runError(testCase, RunnerSummary.format("formatValue"));
            } finally {
                super.connectorOnStop(prepare);
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return list(support(BatchCountFunction.class, RunnerSummary.format("jsFunctionInNeed", BatchCountRun.jsName)));
    }
}
