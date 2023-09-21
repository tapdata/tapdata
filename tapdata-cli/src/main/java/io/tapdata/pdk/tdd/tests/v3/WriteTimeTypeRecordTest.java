package io.tapdata.pdk.tdd.tests.v3;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.tdd.core.PDKTestBaseV2;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.core.base.TestNode;
import io.tapdata.pdk.tdd.tests.basic.RecordEventExecute;
import io.tapdata.pdk.tdd.tests.support.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Date;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Long;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 时间类型数据写入（依赖WriteRecordFunction和QueryByAdvanceFilter/QueryByFilter）
 * 时间类型数据写入经常会遇到时区的问题， 导致时间发生变化
 * 测试失败按错误上报
 */
@DisplayName("writeTimeTest")
@TapGo(tag = "V3", sort = 10050, debug = false)
public class WriteTimeTypeRecordTest extends PDKTestBaseV2 {
    {
        if (PDKTestBaseV2.testRunning) {
            System.out.println(langUtil.formatLang("writeTimeTest.wait"));
        }
    }

    public static List<SupportFunction> testFunctions() {
        return list(
                supportAny(
                        langUtil.formatLang(anyOneFunFormat, "QueryByAdvanceFilterFunction,QueryByFilterFunction"),
                        QueryByAdvanceFilterFunction.class, QueryByFilterFunction.class),
                support(BatchReadFunction.class, LangUtil.format(inNeedFunFormat, "BatchReadFunction")),
                support(WriteRecordFunction.class, LangUtil.format(inNeedFunFormat, "WriteRecordFunction"))
        );
    }

    {
        targetTable = table(testTableId)
                .add(field("id", JAVA_Long).isPrimaryKey(true).primaryKeyPos(1).tapType(tapNumber().maxValue(BigDecimal.valueOf(Long.MAX_VALUE)).minValue(BigDecimal.valueOf(Long.MIN_VALUE))))
                .add(field("Type_DATE", JAVA_Date).tapType(tapDate()))
                .add(field("Type_DATETIME", "Date_Time").tapType(tapDateTime().fraction(3)))
                .add(field("Type_TIME", "Time").tapType(tapTime().withTimeZone(false)))
                .add(field("Type_YEAR", "Year").tapType(tapYear()));
    }

    private static final int recordCount = 1;

    /**
     * 用例1，写入时间并使用（QueryByFilterFunction）查询时间验证是否正确
     * 以DateTime（GMT-0的时间）数据结构写入一个时间， 成功之后， 使用QueryByFilter查出数据，
     * 在经过值转换， 转成DateTime类型， 然后做比值验证。
     */
    @DisplayName("writeTime.queryFilter")
    @TapTestCase(sort = 1)
    @Test
    public void byQueryByFilter() throws NoSuchMethodException {
        System.out.println(langUtil.formatLang("writeTime.queryFilter.wait"));
        AtomicBoolean hasCreatedTable = new AtomicBoolean(false);
        super.execTest((node, testCase) -> {
            Record[] records = Record.testRecordWithTapTable(super.targetTable, recordCount);
            RecordEventExecute execute = node.recordEventExecute();
            execute.builderRecordCleanBefore(records);

            //创建表并新增数据
            if (!this.createTableAndInsertRecord(node, hasCreatedTable)) {
                return;
            }
            //查询数据，并校验
            Record[] recordCopy = execute.records();
            TapTable targetTableModel = super.getTargetTable(node.connectorNode());
            List<Map<String, Object>> result = super.queryRecords(node, targetTableModel, recordCopy);
            final int filterCount = result.size();
            if (filterCount != recordCount) {
                TapAssert.error(execute.testCase(), langUtil.formatLang("writeTime.queryFilter.fail",
                        recordCount,
                        filterCount,
                        recordCount));
            } else {
                Map<String, Object> resultMap = result.get(0);
                StringBuilder builder = new StringBuilder();
                //node.connectorNode().getCodecsFilterManager().transformToTapValueMap(resultMap, targetTableModel.getNameFieldMap());
                //TapCodecsFilterManager.create(TapCodecsRegistry.create()).transformFromTapValueMap(resultMap);
                boolean equals = super.mapEquals(transform(node, targetTableModel, recordCopy[0]), resultMap, builder, targetTableModel.getNameFieldMap());
                TapAssert.asserts(() -> {
                    Assertions.assertTrue(equals, langUtil.formatLang("writeTime.queryFilter.notEquals",
                            recordCount,
                            filterCount,
                            builder.toString()));
                }).acceptAsWarn(execute.testCase(), langUtil.formatLang("writeTime.queryFilter.succeed",
                        recordCount,
                        filterCount));
            }
        }, (node, testCase) -> {
            //删除表
            if (hasCreatedTable.get()) {
                RecordEventExecute execute = node.recordEventExecute();
                execute.dropTable();
            }
        });
    }

    /**
     * 用例2，写入时间并使用（BatchReadFunction）查出时间验证是否正确
     * 以DateTime（GMT-0的时间）数据结构写入一个时间， 成功之后，
     * 使用batchRead查出数据， 在经过值转换， 转成DateTime类型， 然后做比值验证。
     */
    @DisplayName("writeTime.batchRead")
    @TapTestCase(sort = 2)
    @Test
    public void byBatchRead() throws NoSuchMethodException {
        System.out.println(langUtil.formatLang("writeTime.batchRead.wait"));
        AtomicBoolean hasCreatedTable = new AtomicBoolean(false);
        super.execTest((node, testCase) -> {
            Record[] records = Record.testRecordWithTapTable(super.targetTable, recordCount);
            RecordEventExecute execute = node.recordEventExecute();
            execute.builderRecordCleanBefore(records);

            //创建表并新增数据
            if (!this.createTableAndInsertRecord(node, hasCreatedTable)) {
                return;
            }
            ConnectorNode connectorNode = node.connectorNode();
            TapConnectorContext context = connectorNode.getConnectorContext();
            ConnectorFunctions functions = connectorNode.getConnectorFunctions();
            if (super.verifyFunctions(functions, testCase)) {
                return;
            }
            BatchReadFunction batchReadFun = functions.getBatchReadFunction();
            //使用BatchReadFunction， batchSize为10读出所有数据，
            final int batchSize = 10;
            List<TapEvent> list = new ArrayList<>();
            try {
                batchReadFun.batchRead(context, targetTable, null, batchSize, (events, obj) -> {
                    if (null != events && !events.isEmpty()) list.addAll(events);
                });
            } catch (Throwable throwable) {
                TapAssert.error(testCase, langUtil.formatLang("writeTime.batchRead.throw", recordCount, throwable.getMessage()));
                return;
            }

            //数据条目数需要等于1， 查询出这1条数据，只要能查出来数据就算是正确。
            Record[] recordCopy = execute.records();
            int size = list.size();
            if (size != recordCount) {
                TapAssert.error(testCase, langUtil.formatLang("writeTime.batchRead.fail", recordCount, recordCount, size));
            } else {
                Record record = recordCopy[0];
                TapEvent tapEvent = list.get(0);
                //读出的TapInsertRecordEvent， table， time和after不能为空
                if (Objects.isNull(tapEvent)) {
                    TapAssert.error(testCase, langUtil.formatLang("writeTime.batchRead.notEvent", recordCount));
                } else {
                    TapInsertRecordEvent insertEvent = ((TapInsertRecordEvent) tapEvent);
                    TapInsertRecordEvent table = insertEvent.table(targetTable.getId());
                    if (Objects.isNull(table)) {
                        TapAssert.warn(testCase, langUtil.formatLang("writeTime.batchRead.table.null", recordCount));
                    }
                    Long time = insertEvent.getTime();
                    if (Objects.isNull(time)) {
                        TapAssert.warn(testCase, langUtil.formatLang("writeTime.batchRead.time.null", recordCount));
                    }
                    Map<String, Object> after = insertEvent.getAfter();
                    if (Objects.isNull(after)) {
                        TapAssert.error(testCase, langUtil.formatLang("writeTime.batchRead.after.null", recordCount));
                        return;
                    }
                    TapTable targetTableModel = super.getTargetTable(node.connectorNode());
                    Map<String, Object> result = insertEvent.getAfter();
                    StringBuilder builder = new StringBuilder();
                    TapAssert.asserts(() -> assertTrue(
                            super.mapEquals(transform(node, targetTableModel, record), result, builder, targetTableModel.getNameFieldMap()),
                            langUtil.formatLang("writeTime.batchRead.notEquals", recordCount, builder.toString())
                    )).acceptAsWarn(testCase, langUtil.formatLang("writeTime.batchRead.succeed", recordCount));
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

    private boolean createTableAndInsertRecord(TestNode node, AtomicBoolean hasCreatedTable) {
        RecordEventExecute execute = node.recordEventExecute();
        hasCreatedTable.set(super.createTable(node));
        if (!hasCreatedTable.get()) {
            // 建表失败
            return Boolean.FALSE;
        }
        WriteListResult<TapRecordEvent> insert;
        try {
            //插入数据
            insert = execute.insert();
        } catch (Throwable e) {
            TapAssert.error(execute.testCase(), langUtil.formatLang("writeTime.insertRecord.throw", e.getMessage()));
            return Boolean.FALSE;
        }
        boolean backBool = null != insert && insert.getInsertedCount() == recordCount;
        TapAssert.asserts(() ->
                Assertions.assertTrue(
                        backBool,
                        langUtil.formatLang("writeTime.insertRecord.fail",
                                recordCount,
                                null == insert ? 0 : insert.getInsertedCount(),
                                null == insert ? 0 : insert.getModifiedCount(),
                                null == insert ? 0 : insert.getRemovedCount())
                )
        ).acceptAsError(execute.testCase(), langUtil.formatLang("writeTime.insertRecord",
                recordCount,
                null == insert ? 0 : insert.getInsertedCount(),
                null == insert ? 0 : insert.getModifiedCount(),
                null == insert ? 0 : insert.getRemovedCount()));
        return backBool;
    }
}
