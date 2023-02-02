package io.tapdata.pdk.run.support;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.run.base.PDKBaseRun;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import io.tapdata.pdk.tdd.tests.v2.RecordEventExecute;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;

@DisplayName("batchRead")
@TapGo(sort = 3)
public class BatchCountRun extends PDKBaseRun {
    @DisplayName("batchRead.afterInsert")
    @TapTestCase(sort = 1)
    @Test
    public void batchCount() {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            PDKBaseRun.TestNode prepare = prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            try {
                Method testCase = super.getMethod("batchCount");
                super.connectorOnStart(prepare);
                execute.testCase(testCase);

                ConnectorNode connectorNode = prepare.connectorNode();
                ConnectorFunctions connectorFunctions = connectorNode.getConnectorFunctions();
                BatchCountFunction batchCountFunction = connectorFunctions.getBatchCountFunction();
                if (Objects.nonNull(batchCountFunction)){
                    Map<String,Object> batchCountConfig = (Map<String,Object>)super.debugConfig.get("batch_count");
                    Object tableName = batchCountConfig.get("tableName");
                    if (Objects.isNull(tableName)){
                        //Error cannot get tableName in proprities

                        return;
                    }
                    TapTable table = new TapTable(String.valueOf(tableName),String.valueOf(tableName));
                    long count = batchCountFunction.count(connectorNode.getConnectorContext(), table);

                    System.out.println("[SUCCEED] Table count of table " + String.valueOf(tableName) + "is "+ count);
                }else {
                    //Error cannot support batch count function
                }
            } catch (Throwable exception) {

            }finally {
                super.connectorOnStop(prepare);
            }
        });
    }
}
