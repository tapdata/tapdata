package io.tapdata.pdk.debug.support;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.cli.commands.TapSummary;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.debug.base.PDKBaseDebug;
import io.tapdata.pdk.debug.base.ReadStopException;
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
/**
 *
 * */
@DisplayName("batchRead")
@TapGo(sort = 1)
public class BatchReadDebug extends PDKBaseDebug {
    @DisplayName("batchRead.afterInsert")
    @TapTestCase(sort = 1)
    @Test
    void batchRead() {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            List<TapEvent> list = new ArrayList<>();
            PDKTestBase.TestNode prepare = prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            try {
                Method testCase = super.getMethod("batchRead");
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
                }catch (Throwable throwable){
                    if (!(throwable instanceof ReadStopException)){
                        String message = throwable.getMessage();
                        System.out.println(message);
                    }else {
                        String result = toJson(list);
                        System.out.println(result);
                    }
                }
            } catch (Throwable exception) {

            }finally {
                super.connectorOnStop(prepare);
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return list(support(BatchReadFunction.class,TapSummary.format("BatchReadFunctionNeed")));
    }


    //TapAssert.asserts(() ->
    //        Assertions.assertTrue(
    //                list.size() >= 1,
    //                TapSummary.format("batchRead.batchRead.error", recordCount, batchSize, recordCount, null == list ? 0 : list.size())
    //        )
    //).acceptAsWarn(testCase, TapSummary.format("batchRead.batchRead.succeed", recordCount, batchSize, recordCount, null == list ? 0 : list.size()));

}
