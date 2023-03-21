package io.tapdata.pdk.run.support;

import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connection.CommandCallbackFunction;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
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

@DisplayName("commandRun")
@TapGo(sort = 0)
public class CommandRun extends PDKBaseRun {
    private static final String jsName = RunClassMap.COMMAND_RUN.jsName(0);
    @DisplayName("commandRun.run")
    @TapTestCase(sort = 1)
    @Test
    void command() throws NoSuchMethodException {
        Method testCase = super.getMethod("command");
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
                CommandCallbackFunction commandCallbackFunction = functions.getCommandCallbackFunction();
                Map<String, Object> callback = (Map<String, Object>) Optional.ofNullable(super.debugConfig.get(CommandRun.jsName)).orElse(new HashMap<>());
                Object commandInfo = callback.get("commandInfo");
                if (Objects.isNull(commandInfo)) {
                    super.runError(testCase, RunnerSummary.format("commandRun.notCommandInfo"));
                }
                Map<String, Object> commandInfoMap = (Map<String, Object>) commandInfo;
                Object command = commandInfoMap.get("command");
                Object action = commandInfoMap.get("action");
                Object argMap = commandInfoMap.get("argMap");
                Object time = commandInfoMap.get("time");
                CommandInfo info = new CommandInfo();
                info.setCommand(String.valueOf(command));
                info.setAction(String.valueOf(action));
                info.setArgMap((Map<String, Object>) argMap);
                info.setTime(Long.parseLong(String.valueOf(time)));
                info.setConnectionConfig(context.getConnectionConfig());
                info.setNodeConfig(context.getNodeConfig());
                CommandResult filter = commandCallbackFunction.filter(context, info);
                super.runSucceed(testCase, RunnerSummary.format("formatValue",super.formatPatten(filter)));
            } catch (Throwable throwable) {
                super.runError(testCase, RunnerSummary.format("formatValue",throwable.getMessage()));
            } finally{
                super.connectorOnStop(prepare);
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return list(support(BatchReadFunction.class, RunnerSummary.format("jsFunctionInNeed",CommandRun.jsName)));
    }
}
