package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.entity.WriteListResult;
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
import java.util.Arrays;
import java.util.List;

import static io.tapdata.entity.simplify.TapSimplify.list;

@DisplayName("test.queryByFilterTest")//QueryByFilterFunction基于匹配字段查询（依赖WriteRecordFunction）
@TapGo(sort = 2)
public class QueryByFilterTest extends PDKTestBase {
    private static final String TAG = QueryByFilterTest.class.getSimpleName();

    //private void queryByFilter(TapConnectorContext connectorContext, List<TapFilter> filters, TapTable tapTable, Consumer<List<FilterResult>> listConsumer)
    @Test
    @DisplayName("test.queryByFilterTest.insertWithQuery")//用例1，插入数据能正常查询并进行值比对
    @TapTestCase(sort = 1)
    /**
     * 使用WriteRecordFunction插入一条全类型（覆盖TapType的11中类型数据）数据，通过主键作为匹配参数， 查询出该条数据，
     * 再进行精确和模糊值匹配， 只要能查出来数据就算是正确。
     * 如果值只能通过模糊匹配成功， 报警告指出是靠模糊匹配成功的，
     * 如果连模糊匹配也匹配不上， 也报警告（值对与不对很多时候不好判定）。
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
                    //使用WriteRecordFunction插入一条全类型（覆盖TapType的11中类型数据）数据，通过主键作为匹配参数， 查询出该条数据，
                    insert = recordEventExecute.insert();
                    WriteListResult<TapRecordEvent> finalInsert = insert;
                    TapAssert.asserts(()->
                            Assertions.assertTrue(null != finalInsert && finalInsert.getInsertedCount() == records.length,
                                    TapSummary.format(""))
                    ).acceptAsError(testCase,
                                    TapSummary.format(""));

                    ConnectorFunctions connectorFunctions = prepare.connectorNode().getConnectorFunctions();
                    QueryByFilterFunction queryByFilterFunction = connectorFunctions.getQueryByFilterFunction();
                    //通过主键作为匹配参数， 查询出该条数据，
                    //@TODO filters
                    List<TapFilter> filters = null;
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
                            }));

                    //再进行模糊值匹配， 只要能查出来数据就算是正确。如果值只能通过模糊匹配成功， 报警告指出是靠模糊匹配成功的，
                    //如果连模糊匹配也匹配不上， 也报警告（值对与不对很多时候不好判定）。
                    //@TODO filters
                    filters = null;
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
                            }));

                    //再进行精确， 只要能查出来数据就算是正确。
                    //@TODO filters
                    filters = null;
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
                            }));
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
     * 使用WriteRecordFunction插入一条全类型（覆盖TapType的11中类型数据）数据，通过主键作为匹配参数， 查询出该条数据，
     * 再进行精确和模糊值匹配， 只要能查出来数据就算是正确。
     * 如果值只能通过模糊匹配成功， 报警告指出是靠模糊匹配成功的，
     * 如果连模糊匹配也匹配不上， 也报警告（值对与不对很多时候不好判定）。
     * */
    void queryWithLotTapFilter() throws Throwable{
        consumeQualifiedTapNodeInfo(nodeInfo -> {

        });
    }


    public static List<SupportFunction> testFunctions() {
        return list(
                support(WriteRecordFunction.class, TapSummary.format(inNeedFunFormat,"WriteRecordFunction")),
                support(QueryByFilterFunction.class,TapSummary.format(inNeedFunFormat,"QueryByFilterFunction")),
                support(DropTableFunction.class, TapSummary.format(inNeedFunFormat,"DropTableFunction"))
        );
    }
}
