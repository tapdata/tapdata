package io.tapdata.pdk.run.support;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.run.base.PDKBaseRun;
import io.tapdata.pdk.run.base.ReadStopException;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import io.tapdata.pdk.tdd.tests.v2.RecordEventExecute;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

/**
 *
 * */
@DisplayName("batchRead")
@TapGo(sort = 3)
public class DiscoverSchemaRun extends PDKBaseRun {
    @DisplayName("batchRead.afterInsert")
    @TapTestCase(sort = 1)
    @Test
    public void discoverSchema(){
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            List<TapEvent> list = new ArrayList<>();
            PDKTestBase.TestNode prepare = prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            try {
                Method testCase = super.getMethod("discoverSchema");
                super.connectorOnStart(prepare);
                execute.testCase(testCase);

                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnectorContext context = connectorNode.getConnectorContext();
                ConnectorFunctions functions = connectorNode.getConnectorFunctions();
                if (super.verifyFunctions(functions, testCase)) {
                    return;
                }
                BatchReadFunction batchReadFun = functions.getBatchReadFunction();
                Map<String,Object> batchReadConfig = (Map<String,Object>)super.debugConfig.get("batch_read");
                final int batchSize = (int)batchReadConfig.get("pageSize");
                final String tableName = (String)batchReadConfig.get("tableName");
                final Object offset = batchReadConfig.get("offset");
                TapTable table = new TapTable(tableName,tableName);

                try {
                    batchReadFun.batchRead(context, table, offset, batchSize, (events, obj) -> {
                        if (null != events && !events.isEmpty()) {
                            list.addAll(events);
                            throw new ReadStopException();
                        }
                    });
                    super.connectorOnStop(prepare);
                }catch (Throwable throwable){
                    super.connectorOnStop(prepare);
                    if (!(throwable instanceof ReadStopException)){
                        String message = throwable.getMessage();
                        System.out.println(message);
                    }else {
                        String result = toJson(list, JsonParser.ToJsonFeature.PrettyFormat,JsonParser.ToJsonFeature.WriteMapNullValue);
                        System.out.println(result);
                    }
                }
            } catch (Throwable exception) {

            }
        });
    }
}
