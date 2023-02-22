package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.apis.functions.connector.target.ClearTableFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.support.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static io.tapdata.entity.simplify.TapSimplify.list;

@DisplayName("clearTable")
//ClearTableFunction清除表数据（依赖WriteRecordFunction， BatchCountFunction或者QueryByFilterFunction|QueryByAdvanceFilter）
@TapGo(tag = "V2", sort = 100)
public class ClearTableTest extends PDKTestBase {
    /**
     * 使用WriteRecordFunction写入2条数据，
     * 然后调用ClearTableFunction方法清除该表数据之后，
     * 如果数据源实现了BatchCountFunction方法，
     * 调用BatchCountFunction方法查看该表是否为0；
     * 如果实现了QueryByAdvanceFilter|QueryByFilter，
     * 查询插入的两条数据， 应该查询不到的。
     */
    @DisplayName("clearTable.test")//用例1， 写入数据之后再清除表
    @TapTestCase(sort = 1)
    @Test
    void clear() {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            PDKTestBase.TestNode prepare = prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean hasCreatedTable = false;
            try {
                Method testCase = super.getMethod("clear");
                super.connectorOnStart(prepare);
                execute.testCase(testCase);
                if (!(hasCreatedTable = super.createTable(prepare))) {
                    return;
                }
                //使用WriteRecordFunction写入2条数据，
                final int recordCount = 2;
                Record[] records = Record.testRecordWithTapTable(targetTable, recordCount);
                WriteListResult<TapRecordEvent> insert = execute.builderRecord(records).insert();
                TapAssert.asserts(() ->
                        Assertions.assertTrue(
                                null != insert && insert.getInsertedCount() == recordCount,
                                LangUtil.format("clearTable.insert.error", recordCount, null == insert ? 0 : insert.getInsertedCount())
                        )
                ).acceptAsError(testCase, LangUtil.format("clearTable.insert.succeed", recordCount));
                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnectorContext context = connectorNode.getConnectorContext();
                ConnectorFunctions functions = connectorNode.getConnectorFunctions();
                if (super.verifyFunctions(functions, testCase)) {
                    return;
                }
                ClearTableFunction clear = functions.getClearTableFunction();
                //然后调用ClearTableFunction方法清除该表数据之后，
                String tableId = targetTable.getId();
                TapClearTableEvent event = new TapClearTableEvent();
                event.setTableId(tableId);
                event.setReferenceTime(System.currentTimeMillis());
                clear.clearTable(context, event);
                TapAssert.asserts(() -> Assertions.assertTrue(true)).acceptAsError(testCase, LangUtil.format("clearTable.clean", recordCount));
                //如果数据源实现了BatchCountFunction方法，调用BatchCountFunction方法查看该表是否为0；
                BatchCountFunction batchCountFunction = functions.getBatchCountFunction();
                if (null != batchCountFunction) {
                    long count = batchCountFunction.count(context, targetTable);
                    TapAssert.asserts(() ->
                            Assertions.assertEquals(count, 0, LangUtil.format("clearTable.verifyBatchCountFunction.error", 0, count))
                    ).acceptAsError(testCase, LangUtil.format("clearTable.verifyBatchCountFunction.succeed", count));
                    return;
                }
                //如果实现了QueryByAdvanceFilter|QueryByFilter,查询插入的两条数据，应该查询不到的
                QueryByAdvanceFilterFunction queryByAdvance = functions.getQueryByAdvanceFilterFunction();
                if (null != queryByAdvance) {
                    queryByAdvance.query(
                            context, TapAdvanceFilter.create(), targetTable, consumer ->
                                    TapAssert.asserts(() ->
                                            Assertions.assertTrue(
                                                    null == consumer || null == consumer.getResults() || consumer.getResults().isEmpty(),
                                                    LangUtil.format("clearTable.verifyQueryByAdvanceFilterFunction.error", recordCount))
                                    ).acceptAsWarn(testCase, LangUtil.format("clearTable.verifyQueryByAdvanceFilterFunction.succeed", recordCount))
                    );
                    return;
                }
                QueryByFilterFunction queryByFilter = functions.getQueryByFilterFunction();
                if (null != queryByFilter) {
                    queryByFilter.query(
                            context, list(), targetTable, consumer ->
                                    TapAssert.asserts(() ->
                                            Assertions.assertTrue(
                                                    null == consumer || consumer.isEmpty(),
                                                    LangUtil.format("clearTable.verifyQueryByFilterFunction.error", recordCount))
                                    ).acceptAsWarn(testCase, LangUtil.format("clearTable.verifyQueryByFilterFunction.succeed", recordCount))
                    );
                }
            } catch (Throwable e) {
                throw new RuntimeException(e);
            } finally {
                if (hasCreatedTable) execute.dropTable();
                super.connectorOnStop(prepare);
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return list(
                support(WriteRecordFunction.class, LangUtil.format(inNeedFunFormat, "WriteRecordFunction")),
                supportAny(list(BatchCountFunction.class, QueryByFilterFunction.class, QueryByAdvanceFilterFunction.class), LangUtil.format(anyOneFunFormat, "BatchCountFunction,QueryByFilterFunction,QueryByAdvanceFilterFunction")),
                support(ClearTableFunction.class, LangUtil.format(inNeedFunFormat, "ClearTableFunction"))
        );
    }
}
