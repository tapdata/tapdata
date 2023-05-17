package io.tapdata.pdk.tdd.tests.v3;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.tdd.core.PDKTestBaseV2;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.core.base.TestNode;
import io.tapdata.pdk.tdd.tests.basic.RecordEventExecute;
import io.tapdata.pdk.tdd.tests.support.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * WriteRecord的事件多批次执行（依赖WriteRecordFunction和QueryByAdvanceFilter/QueryByFilter）
 */
@DisplayName("batchWriteTest")
@TapGo(tag = "V3", sort = 10030, debug = false)
public class BatchWriteRecordTest extends PDKTestBaseV2 {
    {
        if (PDKTestBaseV2.testRunning) {
            System.out.println(langUtil.formatLang("batchWriteTest.wait"));
        }
    }

    public static List<SupportFunction> testFunctions() {
        return list(supportAny(
                langUtil.formatLang(anyOneFunFormat, "QueryByAdvanceFilterFunction,QueryByFilterFunction"),
                QueryByAdvanceFilterFunction.class, QueryByFilterFunction.class),
                support(WriteRecordFunction.class, LangUtil.format(inNeedFunFormat, "WriteRecordFunction"))
        );
    }

    private static final int recordCount = 10;

    /**
     * 用例1， 10条数据， 按10批插入
     * 不同主键的数据， 按10批次插入， 每个批次1条数据， 最后验证能否读出这10条数据， 且10条数据都准确
     */
    @DisplayName("batchWrite.batch")
    @TapTestCase(sort = 2)
    @Test
    public void batch() throws NoSuchMethodException {
        System.out.println(langUtil.formatLang("batchWrite.batch.wait"));
        AtomicBoolean hasCreatedTable = new AtomicBoolean(false);
        super.execTest((node, testCase) -> {
            Record[] records = Record.testRecordWithTapTable(super.targetTable, recordCount);
            this.insertRecords(node, hasCreatedTable, records);
            TapTable targetTableModel = super.getTargetTable(node.connectorNode());
            //查询数据，并校验
            List<Map<String, Object>> result = new ArrayList<>();
            for (Record record : insertAfter) {
                Record[] r = new Record[1];
                r[0] = record;
                //查询数据，并校验
                List<Map<String, Object>> res = super.queryRecords(node, targetTableModel, r);
                result.addAll(res);
            }
            final int filterCount = result.size();
            if (filterCount != recordCount) {
                //分批次插入后查询结果不一致-数目不一致
                TapAssert.error(testCase, langUtil.formatLang("batchWrite.batch.query.fail", recordCount, 1, filterCount));
            } else {
                for (int index = 0; index < insertAfter.length; index++) {
                    Map<String, Object> resultMap = result.get(index);
                    //node.connectorNode().getCodecsFilterManager().transformToTapValueMap(insertAfter[index], targetTableModel.getNameFieldMap());
                    //TapCodecsFilterManager.create(TapCodecsRegistry.create()).transformFromTapValueMap(insertAfter[index]);
                    StringBuilder builder = new StringBuilder();
                    boolean equals = super.mapEquals(transform(node, targetTableModel, insertAfter[index]), resultMap, builder, targetTableModel.getNameFieldMap());
                    final int finalIndex = index + 1;
                    TapAssert.asserts(() -> {
                        //分批次插入后查询结果不一致-内容不一致
                        Assertions.assertTrue(equals, langUtil.formatLang("batchWrite.batch.query.notEquals", recordCount, 1, filterCount, finalIndex, builder.toString()));
                    }).acceptAsWarn(testCase, langUtil.formatLang("batchWrite.batch.query.succeed", recordCount, 1, filterCount, finalIndex));
                }
            }
        }, (node, testCase) -> {
            //删除表
            if (hasCreatedTable.get()) {
                RecordEventExecute execute = node.recordEventExecute();
                execute.dropTable();
            }
        });
    }

    Record[] insertAfter = new Record[recordCount];
    private boolean insertRecords(TestNode node, AtomicBoolean hasCreatedTable, Record[] records) {
        RecordEventExecute execute = node.recordEventExecute();
        Method testCase = execute.testCase();
        hasCreatedTable.set(super.createTable(node));
        if (!hasCreatedTable.get()) {
            return false;
        }
        WriteListResult<TapRecordEvent> insert;
        for (int index = 0; index < records.length; index++) {
            execute.builderRecordCleanBefore(records[index]);
            insertAfter[index] = execute.records()[0];
            final int finalIndex = index + 1;
            try {
                //插入数据
                insert = execute.insert();
            } catch (Throwable e) {
                TapAssert.error(testCase, langUtil.formatLang("batchWrite.insertRecord.throw", recordCount, 1, finalIndex, e.getMessage()));
                return false;
            }
            WriteListResult<TapRecordEvent> finalInsert = insert;
            TapAssert.asserts(() ->
                    Assertions.assertFalse(
                            null != finalInsert && finalInsert.getInsertedCount() == recordCount,
                            langUtil.formatLang("batchWrite.insertRecord.fail",
                                    recordCount, 1, finalIndex,
                                    null == finalInsert ? 0 : finalInsert.getInsertedCount(),
                                    null == finalInsert ? 0 : finalInsert.getModifiedCount(),
                                    null == finalInsert ? 0 : finalInsert.getRemovedCount())
                    )
            ).acceptAsError(testCase, langUtil.formatLang("batchWrite.insertRecord",
                    recordCount, 1, finalIndex,
                    null == insert ? 0 : insert.getInsertedCount(),
                    null == insert ? 0 : insert.getModifiedCount(),
                    null == insert ? 0 : insert.getRemovedCount()));
        }
        return true;
    }

}
