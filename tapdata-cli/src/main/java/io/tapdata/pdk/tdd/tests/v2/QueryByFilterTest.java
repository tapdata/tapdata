package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.entity.FilterResult;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
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

@DisplayName("test.queryByFilterTest")//QueryByFilterFunction基于匹配字段查询（依赖WriteRecordFunction）
@TapGo(tag = "V2", sort = 30)
public class QueryByFilterTest extends PDKTestBase {
    private static final String TAG = QueryByFilterTest.class.getSimpleName();
    {
        if (PDKTestBase.testRunning) {
            System.out.println(LangUtil.format("queryByFilterTest.test.wait"));
        }
    }
    @Test
    @DisplayName("test.queryByFilterTest.insertWithQuery")//用例1，插入数据能正常查询并进行值比对
    @TapTestCase(sort = 1)
    /**
     * 使用WriteRecordFunction插入一条全类型（覆盖TapType的11中类型数据）数据，通过主键作为匹配参数，查询出该条数据，
     * 再进行精确和模糊值匹配，只要能查出来数据就算是正确。
     * 如果值只能通过模糊匹配成功，报警告指出是靠模糊匹配成功的，
     * 如果连模糊匹配也匹配不上，也报警告（值对与不对很多时候不好判定）。
     * */
    void insertWithQuery() throws Throwable {
        Method testCase = super.getMethod("insertWithQuery");
        System.out.println(LangUtil.format("queryByFilterTest.insertWithQuery.wait"));
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            boolean hasCreateTable = false;
            try {
                super.connectorOnStart(prepare);
                final int recordCount = 1;
                Record[] records = Record.testRecordWithTapTable(targetTable, recordCount);
                RecordEventExecute recordEventExecute = prepare.recordEventExecute();
                recordEventExecute.testCase(testCase);
                recordEventExecute.builderRecord(records);
                WriteListResult<TapRecordEvent> insert = null;
                try {
                    if (!(hasCreateTable = super.createTable(prepare))) {
                        return;
                    }
                    //使用WriteRecordFunction插入一条全类型（覆盖TapType的11中类型数据）数据，通过主键作为匹配参数，查询出该条数据，
                    insert = recordEventExecute.insert();
                    String tableId = targetTable.getId();
                    WriteListResult<TapRecordEvent> finalInsert = insert;
                    TapAssert.asserts(() ->
                            Assertions.assertTrue(null != finalInsert && finalInsert.getInsertedCount() == records.length,
                                    LangUtil.format("queryByFilter.insert.error", null == finalInsert ? 0 : finalInsert.getInsertedCount(), tableId))
                    ).acceptAsError(
                            testCase,
                            LangUtil.format("batchRead.insert.succeed", null == finalInsert ? 0 : finalInsert.getInsertedCount(), tableId)
                    );

                    ConnectorFunctions connectorFunctions = prepare.connectorNode().getConnectorFunctions();
                    QueryByFilterFunction queryByFilterFunction = connectorFunctions.getQueryByFilterFunction();
                    //通过主键作为匹配参数，查询出该条数据，
                    List<TapFilter> filters = new ArrayList<>();
                    String key = "id";
                    Record[] recordCopy = prepare.recordEventExecute().records();
                    Object value = recordCopy[0].get(key);
                    TapFilter tapFilter = TapFilter.create();
                    tapFilter.setMatch(new DataMap().kv(key, value));
                    filters.add(tapFilter);
                    List<FilterResult> results = new ArrayList<>();
                    queryByFilterFunction.query(
                            prepare.connectorNode().getConnectorContext(),
                            filters,
                            targetTable,
                            filterResults -> {
                                if (null != filterResults) results.addAll(filterResults);
                            }
                    );
                    if (!results.isEmpty()) {
                        TapAssert.asserts(() -> {
                            Assertions.assertEquals(results.size(), 1, LangUtil.format("insertWithQuery.queryById.error", recordCount, key, key, value, recordCount));
                        }).acceptAsWarn(testCase, LangUtil.format("insertWithQuery.queryById.succeed", recordCount, key, key, value));
                    } else {
                        TapAssert.asserts(() -> Assertions.fail(LangUtil.format("insertWithQuery.queryById.noData", recordCount, key, key, value, recordCount))).error(testCase);
                    }

                    TapTable targetTableModel = super.getTargetTable(prepare.connectorNode());
                    if (results.size() == 1) {
                        Record record = recordCopy[0];
                        Map<String, Object> tapEvent = results.get(0).getResult();
                        ConnectorNode connectorNode = prepare.connectorNode();
                        //TapTable targetTable = connectorNode.getConnectorContext().getTableMap().get(connectorNode.getTable());
                        //connectorNode.getCodecsFilterManager().transformToTapValueMap(record, targetTableModel.getNameFieldMap());
                        //TapCodecsFilterManager.create(TapCodecsRegistry.create()).transformFromTapValueMap(record);
                        //TapCodecsFilterManager.create(TapCodecsRegistry.create()).transformFromTapValueMap(tapEvent);
                        StringBuilder builder = new StringBuilder();
                        TapAssert.asserts(() -> assertTrue(
                                mapEquals(transform(prepare, targetTableModel, record), tapEvent, builder, targetTableModel.getNameFieldMap()),
                                LangUtil.format("exact.equals.failed", recordCount, builder.toString())
                        )).acceptAsWarn(testCase, LangUtil.format("exact.equals.succeed", recordCount, builder.toString()));
                    }

                    //再进行模糊值匹配，只要能查出来数据就算是正确。如果值只能通过模糊匹配成功，报警告指出是靠模糊匹配成功的，
                    //如果连模糊匹配也匹配不上，也报警告（值对与不对很多时候不好判定）。
                    filters = new ArrayList<>();
                    TapAdvanceFilter advanceFilter = TapAdvanceFilter.create();
                    filters.add(advanceFilter);
                    queryByFilterFunction.query(
                            prepare.connectorNode().getConnectorContext(),
                            filters,
                            targetTableModel,
                            filterResults -> TapAssert.asserts(() -> {
                            }).acceptAsError(testCase, LangUtil.format(""))
                    );

                    //再进行精确，只要能查出来数据就算是正确。
                    filters = new ArrayList<>();
                    queryByFilterFunction.query(
                            prepare.connectorNode().getConnectorContext(),
                            filters,
                            targetTableModel,
                            filterResults -> TapAssert.asserts(() -> {
                            }).acceptAsError(testCase, LangUtil.format(""))
                    );
                } catch (Throwable e) {
                    TapAssert.asserts(() -> Assertions.fail(LangUtil.format(""))).acceptAsError(
                            testCase,
                            LangUtil.format("")
                    );
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                if (hasCreateTable) {
                    prepare.recordEventExecute().dropTable();
                }
                super.connectorOnStop(prepare);
            }
        });
    }


    @Test
    @DisplayName("test.queryByFilterTest.queryWithLotTapFilter")//用例2，查询数据时，指定多个TapFilter，需要返回多个FilterResult，做一一对应
    @TapTestCase(sort = 2)
    /**
     * 使用WriteRecordFunction插入一条全类型（覆盖TapType的11中类型数据）数据，
     * 通过主键作为匹配参数生成一个TapFilter，
     * 在生成一个一定匹配不上的TapFilter，
     * 这样执行之后， 应该返回两个FilterResult，
     * 一个是成功返回数据， 一个是失败返回空结果，
     * 不应该报错。
     * */
    void queryWithLotTapFilter() throws Throwable {
        Method testCase = super.getMethod("queryWithLotTapFilter");
        System.out.println(LangUtil.format("queryByFilterTest.queryWithLotTapFilter.wait"));
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            prepare.recordEventExecute().testCase(testCase);
            boolean hasCreateTable = false;
            try {
                super.connectorOnStart(prepare);
                if (!(hasCreateTable = super.createTable(prepare))) {
                    return;
                }
                Record[] records = Record.testRecordWithTapTable(targetTable, 1);
                RecordEventExecute recordEventExecute = prepare.recordEventExecute();
                //Method testCase = super.getMethod("insertWithQuery");
                //recordEventExecute.testCase(testCase);
                recordEventExecute.builderRecord(records);
                WriteListResult<TapRecordEvent> insert = null;
                //使用WriteRecordFunction插入一条全类型（覆盖TapType的11中类型数据）数据，通过主键作为匹配参数，查询出该条数据，
                insert = recordEventExecute.insert();
                String tableId = targetTable.getId();
                WriteListResult<TapRecordEvent> finalInsert = insert;
                TapAssert.asserts(() ->
                        Assertions.assertTrue(null != finalInsert && finalInsert.getInsertedCount() == records.length,
                                LangUtil.format("queryByFilter.insert.error", null == finalInsert ? 0 : finalInsert.getInsertedCount(), tableId))
                ).acceptAsError(
                        testCase,
                        LangUtil.format("queryByFilter.insert.error", null == finalInsert ? 0 : finalInsert.getInsertedCount(), tableId)
                );

                TapTable targetTableModel = super.getTargetTable(prepare.connectorNode());
                ConnectorFunctions connectorFunctions = prepare.connectorNode().getConnectorFunctions();
                QueryByFilterFunction queryByFilterFunction = connectorFunctions.getQueryByFilterFunction();
                //通过主键作为匹配参数，查询出该条数据，
                //@TODO filters
                Record[] recordCopy = prepare.recordEventExecute().records();
                List<TapFilter> filters = new ArrayList<>();
                String key = "id";
                Object value = recordCopy[0].get(key);
                TapFilter tapFilter = TapFilter.create();
                tapFilter.setMatch(new DataMap().kv(key, value));

                String key1 = "id";
                Object value1 = 9999999;
                TapFilter tapFilter1 = TapFilter.create();
                tapFilter1.setMatch(new DataMap().kv(key1, value1));

                filters.add(tapFilter);
                filters.add(tapFilter1);
                List<FilterResult> results = new ArrayList<>();
                queryByFilterFunction.query(
                        prepare.connectorNode().getConnectorContext(),
                        filters,
                        targetTableModel,
                        filterResults -> {
                            if (null != filterResults) results.addAll(filterResults);
                        }
                );
                if (results.size() == filters.size()) {
                    FilterResult result1 = results.get(0);
                    FilterResult result2 = results.get(1);
                    TapAssert.asserts(() ->
                            Assertions.assertTrue(
                                    (((result1 == null) || (result1.getResult() == null || result1.getResult().isEmpty())) && result2 != null && result2.getResult() != null) ||
                                            (((result2 == null) || (result2.getResult() == null || result2.getResult().isEmpty())) && result1 != null && result1.getResult() != null),
                                    LangUtil.format("lotFilter.notEquals", filters.size(), key, value, key1, value1, filters.size())
                            )
                    ).acceptAsWarn(
                            testCase,
                            LangUtil.format("lotFilter.equals.succeed", filters.size(), key, value, key1, value1, filters.size())
                    );
                } else {
                    TapAssert.asserts(() -> Assertions.fail(LangUtil.format("lotFilter.notEquals.numError", filters.size(), key, value, key1, value1, filters.size(), results.size()))).error(testCase);
                }
            } catch (Throwable e) {
                throw new RuntimeException(e);
            } finally {
                if (hasCreateTable) {
                    prepare.recordEventExecute().dropTable();
                }
                super.connectorOnStop(prepare);
            }
        });
    }


    public static List<SupportFunction> testFunctions() {
        return list(
                support(WriteRecordFunction.class, LangUtil.format(inNeedFunFormat, "WriteRecordFunction")),
                support(QueryByFilterFunction.class, LangUtil.format(inNeedFunFormat, "QueryByFilterFunction"))
        );
    }
}
