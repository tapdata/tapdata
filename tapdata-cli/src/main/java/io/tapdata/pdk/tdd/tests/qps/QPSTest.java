package io.tapdata.pdk.tdd.tests.qps;

import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.tdd.core.PDKTestBaseV2;
import io.tapdata.pdk.tdd.tests.basic.RecordEventExecute;
import io.tapdata.pdk.tdd.tests.support.Record;
import io.tapdata.pdk.tdd.tests.support.TapAssert;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Author:Skeet
 * Date: 2023/6/15
 **/
@DisplayName("batchPauseAndStream")
@TapGo(tag = "qps", sort = 20000, debug = false, ignore = false, group = "qps")
public class QPSTest extends PDKTestBaseV2 {

    @DisplayName("writeTime.queryFilter")
    @TapTestCase(sort = 1)
    @Test
    public void byQueryByFilter() throws NoSuchMethodException {
        System.out.println(langUtil.formatLang("writeTime.queryFilter.wait"));
        AtomicBoolean hasCreatedTable = new AtomicBoolean(false);
        super.execTest((node, testCase) -> {



//            node.connectorNode().getConnectorFunctions().getWriteRecordFunction().writeRecord(
//                    node.connectorNode().getConnectorContext(),
//
//            );


//            Record[] records = Record.testRecordWithTapTable(super.targetTable, recordCount);
//            RecordEventExecute execute = node.recordEventExecute();
//            execute.builderRecordCleanBefore(records);
//
//            //创建表并新增数据
//            if (!this.createTableAndInsertRecord(node, hasCreatedTable)) {
//                return;
//            }
//
//            //查询数据，并校验
//            Record[] recordCopy = execute.records();
//            TapTable targetTableModel = super.getTargetTable(node.connectorNode());
//            List<Map<String, Object>> result = super.queryRecords(node, targetTableModel, recordCopy);
//            final int filterCount = result.size();
//            if (filterCount != recordCount) {
//            TapAssert
//                TapAssert.error(testCase, langUtil.formatLang("writeTime.queryFilter.fail",
//                        recordCount,
//                        filterCount,
//                        recordCount));
//            } else {
//                Map<String, Object> resultMap = result.get(0);
//                StringBuilder builder = new StringBuilder();
//                //node.connectorNode().getCodecsFilterManager().transformToTapValueMap(resultMap, targetTableModel.getNameFieldMap());
//                //TapCodecsFilterManager.create(TapCodecsRegistry.create()).transformFromTapValueMap(resultMap);
//                boolean equals = super.mapEquals(transform(node, targetTableModel, recordCopy[0]), resultMap, builder, targetTableModel.getNameFieldMap());
//                TapAssert.asserts(() -> {
//                    Assertions.assertTrue(equals, langUtil.formatLang("writeTime.queryFilter.notEquals",
//                            recordCount,
//                            filterCount,
//                            builder.toString()));
//                }).acceptAsWarn(execute.testCase(), langUtil.formatLang("writeTime.queryFilter.succeed",
//                        recordCount,
//                        filterCount));
//            }
        }, (node, testCase) -> {
            //删除表
            if (hasCreatedTable.get()) {
                RecordEventExecute execute = node.recordEventExecute();
                execute.dropTable();
            }
        });
    }
}
