package io.tapdata.pdk.run.support;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connection.CommandCallbackFunction;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
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
import java.util.Objects;

import static io.tapdata.entity.simplify.TapSimplify.list;
import static io.tapdata.entity.simplify.TapSimplify.toJson;

@DisplayName("")
@TapGo
public class CommandRun extends PDKBaseRun {
    @DisplayName("batchRead.afterInsert")
    @TapTestCase(sort = 1)
    @Test
    void command() {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            PDKTestBase.TestNode prepare = prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            try {
                Method testCase = super.getMethod("command");
                super.connectorOnStart(prepare);
                execute.testCase(testCase);

                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnectorContext context = connectorNode.getConnectorContext();
                ConnectorFunctions functions = connectorNode.getConnectorFunctions();
                if (super.verifyFunctions(functions, testCase)) {
                    return;
                }
                CommandCallbackFunction commandCallbackFunction = functions.getCommandCallbackFunction();
                Map<String,Object> callback = (Map<String,Object>)super.debugConfig.get("command_callback");
                Object commandInfo = callback.get("commandInfo");
                if (Objects.isNull(commandInfo)){
                    //@TODO ERROR
                }
                Map<String,Object> commandInfoMap = (Map<String, Object>)commandInfo;
                Object command = commandInfoMap.get("command");
                Object action = commandInfoMap.get("action");
                Object argMap = commandInfoMap.get("argMap");
                Object time = commandInfoMap.get("time");
                CommandInfo info = new CommandInfo();
                info.setCommand(String.valueOf(command));
                info.setAction(String.valueOf(action));
                info.setArgMap((Map<String, Object>) argMap);
                info.setTime((Long) time);
                CommandResult filter = commandCallbackFunction.filter(context, info);
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
