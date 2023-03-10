package io.tapdata.pdk.run.support;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;
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
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.list;

@DisplayName("streamReadRun")
@TapGo
public class StreamReadRun extends PDKBaseRun {
    private static final String jsName = RunClassMap.STREAM_READ_RUN.jsName(0);
    @DisplayName("streamReadRun.run")
    @TapTestCase(sort = 1)
    @Test
    void streamRead() throws NoSuchMethodException {
        Method testCase = super.getMethod("streamRead");
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
                StreamReadFunction streamRead = functions.getStreamReadFunction();
                Map<String, Object> batchReadConfig = (Map<String, Object>) Optional.ofNullable(super.debugConfig.get(StreamReadRun.jsName)).orElse(new HashMap<>());
                final int pageSize = (int) batchReadConfig.get("pageSize");
                final List<String> tableName = (List<String>) batchReadConfig.get("tableNameList");
                final Object offset = batchReadConfig.get("offset");
                AtomicReference<StreamReadConsumer> streamReadConsumer = new AtomicReference<>();
                streamReadConsumer.set(StreamReadConsumer.create((events, offsetState) -> {
                    if (Objects.nonNull(events) && !events.isEmpty()) {
                        list.addAll(events);
                        throw new ReadStopException();
                    }
                }));
                streamRead.streamRead(context, tableName, offset, pageSize, streamReadConsumer.get());
            } catch (Throwable throwable) {
                if (!(throwable instanceof ReadStopException)) {
                    super.runError(testCase, RunnerSummary.format("formatValue",throwable.getMessage()));
                } else {
                    super.runSucceed(testCase, RunnerSummary.format("formatValue",super.formatPatten(list)));
                }
            } finally {
                super.connectorOnStop(prepare);
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return list(support(StreamReadFunction.class, RunnerSummary.format("jsFunctionInNeed",StreamReadRun.jsName)));
    }
}
