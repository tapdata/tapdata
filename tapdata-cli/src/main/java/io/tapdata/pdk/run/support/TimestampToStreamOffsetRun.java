package io.tapdata.pdk.run.support;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.TimestampToStreamOffsetFunction;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.tapdata.entity.simplify.TapSimplify.list;

@DisplayName("timestampToStreamOffsetRun")
@TapGo(sort = 6)
public class TimestampToStreamOffsetRun extends PDKBaseRun {
    private static final String jsName = RunClassMap.TIMESTAMP_TO_STREAM_OFFSET_RUN.jsName(0) ;
    @DisplayName("timestampToStreamOffsetRun.run")
    @TapTestCase(sort = 1)
    @Test
    void timestamp() throws NoSuchMethodException {
        Method testCase = super.getMethod("timestamp");
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
                TimestampToStreamOffsetFunction timestamp = functions.getTimestampToStreamOffsetFunction();
                Map<String, Object> batchReadConfig = (Map<String, Object>) Optional.ofNullable(super.debugConfig.get(TimestampToStreamOffsetRun.jsName)).orElse(new HashMap<>());
                final long time = Long.parseLong(String.valueOf(Optional.ofNullable(batchReadConfig.get("time")).orElse(0)));
                Object timestampResult = timestamp.timestampToStreamOffset(context, time);
                super.runSucceed(testCase, RunnerSummary.format("formatValue", super.formatPatten(timestampResult)));
            } catch (Throwable throwable) {
                super.runError(testCase, RunnerSummary.format("formatValue", throwable.getMessage()));
            } finally {
                super.connectorOnStop(prepare);
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return list(support(TimestampToStreamOffsetFunction.class, RunnerSummary.format("jsFunctionInNeed", TimestampToStreamOffsetRun.jsName)));
    }
}
