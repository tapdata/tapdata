package io.tapdata.pdk.run.support;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.source.RawDataCallbackFilterFunctionV2;
import io.tapdata.pdk.cli.commands.TapSummary;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.run.base.PDKBaseRun;
import io.tapdata.pdk.run.base.ReadStopException;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import io.tapdata.pdk.tdd.tests.v2.RecordEventExecute;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.list;
import static io.tapdata.entity.simplify.TapSimplify.toJson;

@DisplayName("")
@TapGo
public class WebHookEventRun extends PDKBaseRun {
    @DisplayName("batchRead.afterInsert")
    @TapTestCase(sort = 1)
    @Test
    void webhook() {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            PDKTestBase.TestNode prepare = prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            try {
                Method testCase = super.getMethod("webhook");
                super.connectorOnStart(prepare);
                execute.testCase(testCase);

                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnectorContext context = connectorNode.getConnectorContext();
                ConnectorFunctions functions = connectorNode.getConnectorFunctions();
                if (super.verifyFunctions(functions, testCase)) {
                    return;
                }
                RawDataCallbackFilterFunctionV2 webhook = functions.getRawDataCallbackFilterFunctionV2();
                Map<String,Object> batchReadConfig = (Map<String,Object>)super.debugConfig.get("batch_read");
                final List<String> tableNames = (List<String>)batchReadConfig.get("tableNameList");
                final Map<String,Object> dataMap = (Map<String,Object>)batchReadConfig.get("eventDataMap");

                List<TapEvent> filter = webhook.filter(context, tableNames, dataMap);
                String result = toJson(filter, JsonParser.ToJsonFeature.PrettyFormat,JsonParser.ToJsonFeature.WriteMapNullValue);
                System.out.println(result);
            } catch (Throwable throwable) {
                String message = throwable.getMessage();
                System.out.println(message);
            }finally {
                super.connectorOnStop(prepare);
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return list(support(BatchReadFunction.class, TapSummary.format("BatchReadFunctionNeed")));
    }
}
