package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.DropTableFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.cli.commands.TapSummary;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.tests.support.Record;
import io.tapdata.pdk.tdd.tests.support.TapAssert;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.tapdata.entity.simplify.TapSimplify.list;

@DisplayName("test.queryByFilterTest")//QueryByFilterFunction基于匹配字段查询（依赖WriteRecordFunction）
@TapGo(sort = 2)
public class QueryByFilterTest extends PDKTestBase {
    private static final String TAG = QueryByFilterTest.class.getSimpleName();

    @Test
    @DisplayName("test.queryByFilterTest.insertWithQuery")//用例1，插入数据能正常查询并进行值比对
    @TapTestCase(sort = 1)
    /**
     * 使用WriteRecordFunction插入一条全类型（覆盖TapType的11中类型数据）数据，通过主键作为匹配参数，查询出该条数据，
     * 再进行精确和模糊值匹配，只要能查出来数据就算是正确。
     * 如果值只能通过模糊匹配成功，报警告指出是靠模糊匹配成功的，
     * 如果连模糊匹配也匹配不上，也报警告（值对与不对很多时候不好判定）。
     * */
    void insertWithQuery() throws Throwable{
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            try {
                super.connectorOnStart(prepare);
                Record[] records = Record.testRecordWithTapTable(targetTable, 1);
                RecordEventExecute recordEventExecute = prepare.recordEventExecute();
                Method testCase = super.getMethod("insertWithQuery");
                recordEventExecute.testCase(testCase);
                recordEventExecute.builderRecord(records);
                WriteListResult<TapRecordEvent> insert = null;
                try {
                    //使用WriteRecordFunction插入一条全类型（覆盖TapType的11中类型数据）数据，通过主键作为匹配参数，查询出该条数据，
                    insert = recordEventExecute.insert();
                    String tableId = targetTable.getId();
                    WriteListResult<TapRecordEvent> finalInsert = insert;
                    TapAssert.asserts(()->
                        Assertions.assertTrue(null != finalInsert && finalInsert.getInsertedCount() == records.length,
                                TapSummary.format("queryByFilter.insert.error",null==finalInsert?0:finalInsert.getInsertedCount(),tableId))
                    ).acceptAsError(
                        testCase,
                        TapSummary.format("batchRead.insert.succeed",null==finalInsert?0:finalInsert.getInsertedCount(),tableId)
                    );

                    ConnectorFunctions connectorFunctions = prepare.connectorNode().getConnectorFunctions();
                    QueryByFilterFunction queryByFilterFunction = connectorFunctions.getQueryByFilterFunction();
                    //通过主键作为匹配参数，查询出该条数据，
                    List<TapFilter> filters = new ArrayList<>();
                    String key = "id";
                    Object value = records[0].get(key);
                    TapFilter tapFilter = TapFilter.create();
                    tapFilter.setMatch(new DataMap().kv(key,value));
                    filters.add(tapFilter);
                    List<FilterResult> results = new ArrayList<>();
                    queryByFilterFunction.query(
                        prepare.connectorNode().getConnectorContext(),
                        filters,
                        targetTable,
                        filterResults -> {
                            if (null!=filterResults) results.addAll(filterResults);
                        }
                    );
                    if (!results.isEmpty()){
                        TapAssert.asserts(()->{
                            Assertions.assertTrue(1!=results.size(),TapSummary.format(""));
                        }).acceptAsError(
                            testCase,
                            TapSummary.format("")
                        );
                    }else {
                        TapAssert.asserts(()-> Assertions.fail(TapSummary.format(""))).error(testCase);
                    }

                    //再进行模糊值匹配，只要能查出来数据就算是正确。如果值只能通过模糊匹配成功，报警告指出是靠模糊匹配成功的，
                    //如果连模糊匹配也匹配不上，也报警告（值对与不对很多时候不好判定）。
                    filters = new ArrayList<>();
                    TapAdvanceFilter advanceFilter = TapAdvanceFilter.create();
                    //advanceFilter.match(new DataMap().kv("",""));
                    filters.add(advanceFilter);
                    queryByFilterFunction.query(
                        prepare.connectorNode().getConnectorContext(),
                        filters,
                        targetTable,
                        filterResults -> $(() ->
                            TapAssert.asserts(()->{

                            }).acceptAsError(
                                testCase,
                                TapSummary.format("")
                            )
                        )
                    );

                    //再进行精确，只要能查出来数据就算是正确。
                    filters = new ArrayList<>();
                    queryByFilterFunction.query(
                        prepare.connectorNode().getConnectorContext(),
                        filters,
                        targetTable,
                        filterResults -> $(() -> {
                            TapAssert.asserts(()->{

                            }).acceptAsError(
                                testCase,
                                TapSummary.format("")
                            );
                        })
                    );
                }catch (Throwable e){
                    TapAssert.asserts(()->
                        Assertions.fail(TapSummary.format(""))
                    ).acceptAsError(
                        testCase,
                        TapSummary.format("")
                    );
                }
            }catch (Exception e){
                throw new RuntimeException(e);
            }finally {
                prepare.recordEventExecute().dropTable();
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
    void queryWithLotTapFilter() throws Throwable{
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            try {
                super.connectorOnStart(prepare);
                Record[] records = Record.testRecordWithTapTable(targetTable, 1);
                RecordEventExecute recordEventExecute = prepare.recordEventExecute();
                Method testCase = super.getMethod("insertWithQuery");
                recordEventExecute.testCase(testCase);
                recordEventExecute.builderRecord(records);
                WriteListResult<TapRecordEvent> insert = null;
                //使用WriteRecordFunction插入一条全类型（覆盖TapType的11中类型数据）数据，通过主键作为匹配参数，查询出该条数据，
                insert = recordEventExecute.insert();
                String tableId = targetTable.getId();
                WriteListResult<TapRecordEvent> finalInsert = insert;
                TapAssert.asserts(()->
                    Assertions.assertTrue(null != finalInsert && finalInsert.getInsertedCount() == records.length,
                            TapSummary.format("queryByFilter.insert.error",null==finalInsert?0:finalInsert.getInsertedCount(),tableId))
                ).acceptAsError(
                    testCase,
                    TapSummary.format("queryByFilter.insert.error",null==finalInsert?0:finalInsert.getInsertedCount(),tableId)
                );

                ConnectorFunctions connectorFunctions = prepare.connectorNode().getConnectorFunctions();
                QueryByFilterFunction queryByFilterFunction = connectorFunctions.getQueryByFilterFunction();
                //通过主键作为匹配参数，查询出该条数据，
                //@TODO filters
                List<TapFilter> filters = new ArrayList<>();
                String key = "id";
                Object value = records[0].get(key);
                TapFilter tapFilter = TapFilter.create();
                tapFilter.setMatch(new DataMap().kv(key,value));

                String key1 = "id";
                Object value1 = "can not match data";
                TapFilter tapFilter1 = TapFilter.create();
                tapFilter.setMatch(new DataMap().kv(key1,value1));

                filters.add(tapFilter);
                filters.add(tapFilter1);
                List<FilterResult> results = new ArrayList<>();
                queryByFilterFunction.query(
                    prepare.connectorNode().getConnectorContext(),
                    filters,
                    targetTable,
                    filterResults -> {
                        if (null!=filterResults) results.addAll(filterResults);
                    }
                );
                if (results.size()!=filters.size()){
                    FilterResult result1 = results.get(0);
                    FilterResult result2 = results.get(1);
                    TapAssert.asserts(()->
                        Assertions.assertTrue(
                        ( ((result1==null) || ( result1.getResult()==null || result1.getResult().isEmpty())) && result2 != null && result2.getResult() != null ) ||
                                ( ((result2==null) || ( result2.getResult()==null || result2.getResult().isEmpty())) && result1 != null && result1.getResult() != null),
                            TapSummary.format("lotFilter.notEquals",filters.size(),key,value,key1,value1,filters.size())
                        )
                    ).acceptAsWarn(
                        testCase,
                        TapSummary.format("lotFilter.equals.succeed",filters.size(),key,value,key1,value1,filters.size())
                    );
                }else {
                    TapAssert.asserts(()-> Assertions.fail(TapSummary.format("lotFilter.notEquals.numError",filters.size(),key,value,key1,value1,filters.size(),results.size()))).error(testCase);
                }

            }catch (Throwable e){
                throw new RuntimeException(e);
            }finally {
                prepare.recordEventExecute().dropTable();
                super.connectorOnStop(prepare);
            }
        });
    }


    public static List<SupportFunction> testFunctions() {
        return list(
                support(WriteRecordFunction.class, TapSummary.format(inNeedFunFormat,"WriteRecordFunction")),
                support(QueryByFilterFunction.class,TapSummary.format(inNeedFunFormat,"QueryByFilterFunction"))
//                support(DropTableFunction.class, TapSummary.format(inNeedFunFormat,"DropTableFunction"))
        );
    }
}
