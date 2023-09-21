package io.tapdata.pdk.run.support;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.RawDataCallbackFilterFunctionV2;
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

@DisplayName("webHookEventRun")
@TapGo(sort = 8)
public class WebHookEventRun extends PDKBaseRun {
    public static final String jsName = RunClassMap.WEB_HOOK_EVENT_RUN.jsName(0);
    @DisplayName("webHookEventRun.run")
    @TapTestCase(sort = 1)
    @Test
    void webhook() throws NoSuchMethodException {
        Method testCase = super.getMethod("webhook");
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
                RawDataCallbackFilterFunctionV2 webhook = functions.getRawDataCallbackFilterFunctionV2();
                Map<String, Object> batchReadConfig = (Map<String, Object>) Optional.ofNullable(super.debugConfig.get(WebHookEventRun.jsName)).orElse(new HashMap<>());
                final List<String> tableNames = (List<String>) batchReadConfig.get("tableNameList");
                final Map<String, Object> dataMap = (Map<String, Object>) batchReadConfig.get("eventDataMap");

                List<TapEvent> filter = webhook.filter(context, tableNames, dataMap);
                super.runSucceed(testCase, RunnerSummary.format("formatValue",super.formatPatten(filter)));
            } catch (Throwable throwable) {
                super.runError(testCase, RunnerSummary.format("formatValue",throwable.getMessage()));
            } finally {
                super.connectorOnStop(prepare);
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return list(support(RawDataCallbackFilterFunctionV2.class, RunnerSummary.format("jsFunctionInNeed",WebHookEventRun.jsName)));
    }
}
