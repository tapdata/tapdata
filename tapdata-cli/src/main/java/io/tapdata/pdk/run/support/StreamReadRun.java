package io.tapdata.pdk.run.support;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;
import io.tapdata.pdk.cli.commands.TapSummary;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.error.PDKRunnerErrorCodes;
import io.tapdata.pdk.core.utils.LoggerUtils;
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
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.list;
import static io.tapdata.entity.simplify.TapSimplify.toJson;

@DisplayName("")
@TapGo
public class StreamReadRun extends PDKBaseRun {
    @DisplayName("batchRead.afterInsert")
    @TapTestCase(sort = 1)
    @Test
    void streamRead() {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            List<TapEvent> list = new ArrayList<>();
            PDKTestBase.TestNode prepare = prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            try {
                Method testCase = super.getMethod("streamRead");
                super.connectorOnStart(prepare);
                execute.testCase(testCase);

                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnectorContext context = connectorNode.getConnectorContext();
                ConnectorFunctions functions = connectorNode.getConnectorFunctions();
                if (super.verifyFunctions(functions, testCase)) {
                    return;
                }
                StreamReadFunction streamRead = functions.getStreamReadFunction();
                Map<String,Object> batchReadConfig = (Map<String,Object>)super.debugConfig.get("stream_read");
                final int pageSize = (int)batchReadConfig.get("pageSize");
                final List<String> tableName = (List<String>)batchReadConfig.get("tableNameList");
                final Object offset = batchReadConfig.get("offset");
                AtomicReference<StreamReadConsumer> streamReadConsumer = new AtomicReference<>();
                streamReadConsumer.set(StreamReadConsumer.create((events, offsetState) -> {
                    if (Objects.nonNull(events) && !events.isEmpty()){
                        list.addAll(events);
                        throw new ReadStopException();
                    }
                }));
                streamRead.streamRead(context, tableName, offset, pageSize,streamReadConsumer.get());
            } catch (Throwable throwable) {
                if (!(throwable instanceof ReadStopException)){
                    String message = throwable.getMessage();
                    System.out.println(message);
                }else {
                    String result = toJson(list, JsonParser.ToJsonFeature.PrettyFormat,JsonParser.ToJsonFeature.WriteMapNullValue);
                    System.out.println(result);
                }
            }finally {
                super.connectorOnStop(prepare);
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return list(support(BatchReadFunction.class, TapSummary.format("BatchReadFunctionNeed")));
    }
}
