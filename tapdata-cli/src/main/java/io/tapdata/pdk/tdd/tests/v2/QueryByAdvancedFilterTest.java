package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.tdd.core.PDKTestBase;
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

import static io.tapdata.entity.simplify.TapSimplify.list;
import static org.junit.jupiter.api.Assertions.assertTrue;


@DisplayName("queryByAdvanced")//QueryByAdvancedFilterFunction 基于匹配字段高级查询（依赖WriteRecordFunction）
@TapGo(tag = "V2", sort = 40)//都需使用随机ID建表， 如果有DropTableFunction实现， 测试用例应该自动删除创建的临时表（无论成功或是失败）
public class QueryByAdvancedFilterTest extends PDKTestBase {
    private static final String TAG = QueryByAdvancedFilterTest.class.getSimpleName();

    {
        if (PDKTestBase.testRunning) {
            System.out.println(LangUtil.format("queryByAdvanced.test.wait"));
        }
    }

    @Test
    @DisplayName("test.byAdvance.sourceTest")
    @TapTestCase(sort = 1)
    /**
     * 使用WriteRecordFunction插入一条全类型（覆盖TapType的11中类型数据）数据，通过主键作为匹配参数，
     * 查询出该条数据， 再进行精确和模糊值匹配， 只要能查出来数据就算是正确。
     * 如果值只能通过模糊匹配成功， 报警告指出是靠模糊匹配成功的，
     * 如果连模糊匹配也匹配不上， 也报警告（值对与不对很多时候不好判定）。
     * */
    void sourceTest() throws NoSuchMethodException {
        System.out.println(LangUtil.format("queryByAdvanced.sourceTest.wait"));
        Method testCase = super.getMethod("sourceTest");
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean hasCreateTable = false;
            super.connectorOnStart(prepare);
            execute.testCase(testCase);
            if (!(hasCreateTable = super.createTable(prepare))) {
                super.connectorOnStop(prepare);
                return;
            }
            //使用WriteRecordFunction插入1条全类型（覆盖TapType的11中类型数据）数据，
            final int recordCount = 1;
            Record[] records = Record.testRecordWithTapTable(targetTable, recordCount);
            WriteListResult<TapRecordEvent> insert = null;
            try {
                insert = execute.builderRecord(records).insert();
            } catch (Throwable e) {
                if (hasCreateTable) execute.dropTable();
                super.connectorOnStop(prepare);
                TapAssert.error(testCase, LangUtil.format("fieldModification.all.throw", e.getMessage()));
                return;
            }
            try {
                WriteListResult<TapRecordEvent> finalInsert = insert;
                TapAssert.asserts(() ->
                        Assertions.assertTrue(
                                null != finalInsert && finalInsert.getInsertedCount() == recordCount,
                                LangUtil.format("batchRead.insert.error", recordCount, null == finalInsert ? 0 : finalInsert.getInsertedCount())
                        )
                ).acceptAsError(testCase, LangUtil.format("batchRead.insert.succeed", recordCount, null == insert ? 0 : insert.getInsertedCount()));

                ConnectorNode connectorNode = prepare.connectorNode();
                ConnectorFunctions functions = connectorNode.getConnectorFunctions();
                if (super.verifyFunctions(functions, testCase)) {
                    return;
                }
                QueryByAdvanceFilterFunction query = functions.getQueryByAdvanceFilterFunction();
                TapAdvanceFilter queryFilter = new TapAdvanceFilter();


                List<Map<String, Object>> consumer = null;
                try {
                    consumer = filter(connectorNode, query, queryFilter);
                } catch (Throwable e) {
                    TapAssert.error(testCase, LangUtil.format("fieldModification.all.throw", e.getMessage()));
                    return;
                }

                Record[] recordCopy = execute.records();
                //数据条目数需要等于1， 查询出这1条数据，只要能查出来数据就算是正确。
                List<Map<String, Object>> finalConsumer = consumer;
                TapAssert.asserts(() ->
                        Assertions.assertEquals(
                                finalConsumer.size(), recordCount,
                                LangUtil.format("byAdvance.query.error", recordCount, null == finalConsumer ? 0 : finalConsumer.size())
                        )
                ).acceptAsError(testCase, LangUtil.format("byAdvance.query.succeed", recordCount, null == consumer ? 0 : consumer.size()));
                if (consumer.size() == 1) {
                    Record record = recordCopy[0];
                    TapTable targetTableModel = super.getTargetTable(connectorNode);
                    Map<String, Object> tapEvent = consumer.get(0);
                    Map<String, Object> result = tapEvent;//filterResult.getResult();
                    //connectorNode.getCodecsFilterManager().transformToTapValueMap(result, targetTableModel.getNameFieldMap());
                    //TapCodecsFilterManager.create(TapCodecsRegistry.create()).transformFromTapValueMap(result);
                    StringBuilder builder = new StringBuilder();
                    TapAssert.asserts(() -> assertTrue(
                            mapEquals(transform(prepare, targetTableModel, record), result, builder, targetTableModel.getNameFieldMap()),
                            LangUtil.format("exact.equals.failed", recordCount, builder.toString())
                    )).acceptAsWarn(testCase, LangUtil.format("exact.equals.succeed", recordCount, builder.toString()));
                }
            } finally {
                if (hasCreateTable) {
                    execute.dropTable(targetTable);
                }
                super.connectorOnStop(prepare);
            }
        });
    }

    @Test
    @DisplayName("test.byAdvance.sourceTest2")
    @TapTestCase(sort = 2)
    /**
     * 使用WriteRecordFunction插入两条不同的条全类型（覆盖TapType的11中类型数据）数据，
     * 通过TapAdvanceFilter的功能进行匹配， 操作符， 排序， Projection等测试，
     * 只要能按预期匹配到数据， 或者匹配不到数据即可。 此用例不用做值比对
     * */
    void sourceTest2() throws Throwable {
        System.out.println(LangUtil.format("queryByAdvanced.sourceTest2.wait"));
        Method testCase = this.getMethod("sourceTest2");
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean hasCreateTable = false;
            execute.testCase(testCase);
            super.connectorOnStart(prepare);

            if (!(hasCreateTable = super.createTable(prepare,false))) {
                super.connectorOnStop(prepare);
                return;
            }
            final int insertCount = 2;
            Record[] records = Record.testRecordWithTapTable(targetTable, insertCount);
            execute.builderRecord(records);
            WriteListResult<TapRecordEvent> insert = null;
            try {
                insert = execute.insert();
            } catch (Throwable e) {
                execute.dropTable();
                super.connectorOnStop(prepare);
                TapAssert.error(testCase, LangUtil.format("fieldModification.all.throw", e.getMessage()));
                return;
            }
            try {

                WriteListResult<TapRecordEvent> finalInsert = insert;
                TapAssert.asserts(() ->
                        Assertions.assertTrue(
                                null != finalInsert && finalInsert.getInsertedCount() == insertCount,
                                LangUtil.format("batchRead.insert.error", insertCount, null == finalInsert ? 0 : finalInsert.getInsertedCount())
                        )
                ).acceptAsError(testCase, LangUtil.format("batchRead.insert.succeed", insertCount, null == insert ? 0 : insert.getInsertedCount()));
                ConnectorNode connectorNode = prepare.connectorNode();
                ConnectorFunctions functions = connectorNode.getConnectorFunctions();
                if (super.verifyFunctions(functions, testCase)) {
                    return;
                }
                String tableId = targetTable.getId();
                QueryByAdvanceFilterFunction query = null;
                try {
                    query = functions.getQueryByAdvanceFilterFunction();
                } catch (Throwable e) {
                    TapAssert.error(testCase, LangUtil.format("fieldModification.all.throw", e.getMessage()));
                    return;
                }

                String key = "id";
                Record[] recordCopy = execute.records();
                Long value = (Long) recordCopy[0].get(key);
                try {
                    this.operatorEq(key, value, testCase, connectorNode, query);
                    this.operator(key, value - 1, QueryOperator.GT, testCase, connectorNode, query);
                    this.operator(key, value, QueryOperator.GTE, testCase, connectorNode, query);
                    this.operator(key, value + 1, QueryOperator.LT, testCase, connectorNode, query);
                    this.operator(key, value, QueryOperator.LTE, testCase, connectorNode, query);

                    this.sort(key, SortOn.ASCENDING, testCase, connectorNode, query);
                    this.sort(key, SortOn.DESCENDING, testCase, connectorNode, query);
                } catch (Throwable e) {
                    TapAssert.error(testCase, LangUtil.format("fieldModification.all.throw", e.getMessage()));
                    return;
                }

                TapAdvanceFilter projectionFilter = new TapAdvanceFilter();
                projectionFilter.projection(new Projection().include(key));
                List<Map<String, Object>> projection = null;
                try {
                    projection = filter(connectorNode, query, projectionFilter);
                } catch (Throwable e) {
                    TapAssert.error(testCase, LangUtil.format("fieldModification.all.throw", e.getMessage()));
                    return;
                }
                List<Map<String, Object>> finalProjection = projection;
                TapAssert.asserts(() -> {
                    Assertions.assertFalse(finalProjection.isEmpty(), LangUtil.format("queryByAdvanced.projection.error", key, tableId));
                }).acceptAsError(testCase, LangUtil.format("queryByAdvanced.projection.succeed", key, tableId));
            } finally {
                if (hasCreateTable) execute.dropTable();
                super.connectorOnStop(prepare);
            }
        });
    }

    private void operatorEq(String key, Object value, Method testCase, ConnectorNode connectorNode, QueryByAdvanceFilterFunction query) throws Throwable {
        TapAdvanceFilter operatorFilter = new TapAdvanceFilter();
        String tableId = targetTable.getId();
        operatorFilter.match(new DataMap().kv(key, value));
        List<Map<String, Object>> operator = filter(connectorNode, query, operatorFilter);
        TapAssert.asserts(() -> {
            Assertions.assertFalse(operator.isEmpty(), LangUtil.format("queryByAdvanced.operator.error", "=", key, "=", value, tableId));
        }).acceptAsError(testCase, LangUtil.format("queryByAdvanced.operator.succeed", "=", key, "=", value, tableId));
    }

    private void operator(String key, Object value, int queryOperator, Method testCase, ConnectorNode connectorNode, QueryByAdvanceFilterFunction query) throws Throwable {
        String operatorChar = "?";
        switch (queryOperator) {
            case QueryOperator.GT:
                operatorChar = ">";
                break;
            case QueryOperator.GTE:
                operatorChar = ">=";
                break;
            case QueryOperator.LT:
                operatorChar = "<";
                break;
            case QueryOperator.LTE:
                operatorChar = "<=";
                break;
            default:
                operatorChar = "?";
        }
        TapAdvanceFilter operatorFilter = new TapAdvanceFilter();
        QueryOperator operator = new QueryOperator(key, value, queryOperator);
        String tableId = targetTable.getId();
        operatorFilter.setOperators(list(operator));
        List<Map<String, Object>> operatorRes = filter(connectorNode, query, operatorFilter);
        String finalOperatorChar = operatorChar;
        TapAssert.asserts(() -> {
            Assertions.assertFalse(operatorRes.isEmpty(), LangUtil.format("queryByAdvanced.operator.error", finalOperatorChar, key, finalOperatorChar, value, tableId));
        }).acceptAsError(testCase, LangUtil.format("queryByAdvanced.operator.succeed", finalOperatorChar, key, finalOperatorChar, value, tableId));
    }

    private void sort(String key, int sortOn, Method testCase, ConnectorNode connectorNode, QueryByAdvanceFilterFunction query) throws Throwable {
        TapAdvanceFilter sortFilter = new TapAdvanceFilter();
        sortFilter.sort(new SortOn(key, sortOn));
        String tableId = targetTable.getId();
        List<Map<String, Object>> sort = filter(connectorNode, query, sortFilter);
        TapAssert.asserts(() ->
                Assertions.assertFalse(sort.isEmpty(), LangUtil.format("queryByAdvanced.sort.error", key, sortOn == 2 ? "DESCENDING" : "ASCENDING", tableId))
        ).acceptAsError(testCase, LangUtil.format("queryByAdvanced.sort.succeed", key, sortOn == 2 ? "DESCENDING" : "ASCENDING", tableId));
    }

    private List<Map<String, Object>> filter(ConnectorNode connectorNode, QueryByAdvanceFilterFunction query, TapAdvanceFilter operatorFilter) throws Throwable {
        List<Map<String, Object>> events = new ArrayList<>();
        query.query(
                connectorNode.getConnectorContext(),
                operatorFilter, targetTable, con -> {
                    if (null != con && null != con.getResults() && !con.getResults().isEmpty()) {
                        events.addAll(con.getResults());
                    }
                }
        );
        return events;
    }

    public static List<SupportFunction> testFunctions() {
        return list(
                support(WriteRecordFunction.class, LangUtil.format(inNeedFunFormat, "WriteRecordFunction")),
                support(QueryByAdvanceFilterFunction.class, LangUtil.format(inNeedFunFormat, "QueryByAdvanceFilterFunction"))
        );
    }
}
