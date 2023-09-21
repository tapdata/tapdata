package io.tapdata.pdk.run.support;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.run.base.PDKBaseRun;
import io.tapdata.pdk.run.base.ReadStopException;
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

/**
 *
 */
@DisplayName("batchReadRun")
@TapGo(sort = 5)
public class BatchReadRun extends PDKBaseRun {
    private static final String jsName = RunClassMap.BATCH_READ_RUN.jsName(0);
    @DisplayName("batchReadRun.run")
    @TapTestCase(sort = 1)
    @Test
    void batchRead() throws NoSuchMethodException {
        Method testCase = super.getMethod("batchRead");
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            List<TapEvent> list = new ArrayList<>();
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
                BatchReadFunction batchReadFun = functions.getBatchReadFunction();
                Map<String, Object> batchReadConfig = (Map<String, Object>) Optional.ofNullable(super.debugConfig.get(BatchReadRun.jsName)).orElse(new HashMap<>());
                final int batchSize = (int) batchReadConfig.get("pageSize");
                final String tableName = (String) batchReadConfig.get("tableName");
                final Object offset = batchReadConfig.get("offset");
                TapTable table = new TapTable(tableName, tableName);
                batchReadFun.batchRead(context, table, offset, batchSize, (events, obj) -> {
                    if (null != events && !events.isEmpty()) {
                        list.addAll(events);
                        throw new ReadStopException();
                    }
                });
            } catch (Throwable throwable) {
                if (!(throwable instanceof ReadStopException)) {
                    super.runError(testCase, RunnerSummary.format("formatValue", throwable.getMessage()));
                } else {
                    super.runSucceed(testCase, RunnerSummary.format("formatValue", super.formatPatten(list)));
                }
            } finally {
                super.connectorOnStop(prepare);
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return list(support(BatchReadFunction.class, RunnerSummary.format("jsFunctionInNeed", BatchReadRun.jsName)));
    }
}
