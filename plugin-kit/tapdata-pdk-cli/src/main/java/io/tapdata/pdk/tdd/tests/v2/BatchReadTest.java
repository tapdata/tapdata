package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.FilterResult;
import io.tapdata.pdk.apis.entity.TapFilter;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.target.DropTableFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.cli.commands.TapSummary;
import io.tapdata.pdk.core.api.ConnectorNode;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.tapdata.entity.simplify.TapSimplify.list;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 都需使用随机ID建表， 如果有DropTableFunction实现， 测试用例应该自动删除创建的临时表（无论成功或是失败）
 * */
@DisplayName("batchRead")//BatchReadFunction全量读数据（依赖WriteRecordFunction）
@TapGo(sort = 12)
public class BatchReadTest extends PDKTestBase {
    /**
     * 使用WriteRecordFunction插入1条全类型（覆盖TapType的11中类型数据）数据，
     * 使用BatchReadFunction， batchSize为10读出所有数据，
     * 数据条目数需要等于1， 查询出这1条数据， 再进行精确和模糊值匹配，
     * 只要能查出来数据就算是正确。 如果值只能通过模糊匹配成功，
     * 报警告指出是靠模糊匹配成功的， 如果连模糊匹配也匹配不上， 也报警告（值对与不对很多时候不好判定）。
     *
     * 读出的TapInsertRecordEvent， table， time和after不能为空
     * */
    @DisplayName("batchRead.afterInsert")//用例1，写入1条数据再读出1条数据
    @TapTestCase(sort = 1)
    @Test
    void readAfterInsert(){
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            PDKTestBase.TestNode prepare = prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean hasCreateTable = false;
            try {
                Method testCase = super.getMethod("readAfterInsert");
                super.connectorOnStart(prepare);
                execute.testCase(testCase);
                if (! (hasCreateTable = super.createTable(prepare))){
                    return;
                }
                //使用WriteRecordFunction插入1条全类型（覆盖TapType的11中类型数据）数据，
                final int recordCount = 1;
                Record[] records = Record.testRecordWithTapTable(targetTable,recordCount);
                WriteListResult<TapRecordEvent> insert = execute.builderRecord(records).insert();
                TapAssert.asserts(()->
                    Assertions.assertTrue(
                        null!=insert && insert.getInsertedCount() == recordCount,
                        TapSummary.format("batchRead.insert.error",recordCount,null==insert?0:insert.getInsertedCount())
                    )
                ).acceptAsError(testCase, TapSummary.format("batchRead.insert.succeed",recordCount,null==insert?0:insert.getInsertedCount()));

                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnectorContext context = connectorNode.getConnectorContext();
                ConnectorFunctions functions = connectorNode.getConnectorFunctions();
                if (super.verifyFunctions(functions,testCase)){
                    return;
                }
                BatchReadFunction batchReadFun = functions.getBatchReadFunction();
                //使用BatchReadFunction， batchSize为10读出所有数据，
                final int batchSize = 10;
                batchReadFun.batchRead(context,targetTable,null,batchSize,(list,consumer)->{
                    //数据条目数需要等于1， 查询出这1条数据，只要能查出来数据就算是正确。
                    TapAssert.asserts(()->
                        Assertions.assertTrue(
                    null!=list&&list.size()>=1,
                            TapSummary.format("batchRead.batchRead.error",recordCount,batchSize,recordCount,null==list?0:list.size())
                        )
                    ).acceptAsWarn(testCase,TapSummary.format("batchRead.batchRead.succeed",recordCount,batchSize,recordCount,null==list?0:list.size()));
                    if (null != list && list.size() == 1){
                        Record record = records[0];
                        TapEvent tapEvent = list.get(0);
                        //读出的TapInsertRecordEvent， table， time和after不能为空
                        TapAssert.asserts(()->{
                            Assertions.assertNotNull(tapEvent,TapSummary.format("batchRead.tapInsertRecordEvent.null"));
                        }).acceptAsError(testCase,TapSummary.format("batchRead.tapInsertRecordEvent.notNull"));
                        if (null!=tapEvent) {
                            TapInsertRecordEvent insertEvent = ((TapInsertRecordEvent) tapEvent);
                            TapInsertRecordEvent table = insertEvent.table(targetTable.getId());
                            TapAssert.asserts(() -> {
                                Assertions.assertNotNull(table,TapSummary.format("batchRead.table.null"));
                            }).acceptAsError(testCase, TapSummary.format("batchRead.table.notNull"));
                            Long time = insertEvent.getTime();
                            TapAssert.asserts(() -> {
                                Assertions.assertNotNull(time,TapSummary.format("batchRead.time.null"));
                            }).acceptAsError(testCase, TapSummary.format("batchRead.time.notNull"));
                            Map<String, Object> after = insertEvent.getAfter();
                            TapAssert.asserts(() -> {
                                Assertions.assertNotNull(after,TapSummary.format("batchRead.after.null"));
                            }).acceptAsError(testCase, TapSummary.format("batchRead.after.notNull"));
                        }


                        Map<String, Object> info = tapEvent.getInfo();

                        DataMap filterMap = (DataMap) info;
                        TapFilter filter = new TapFilter();
                        filter.setMatch(filterMap);
                        TapTable targetTable = connectorNode.getConnectorContext().getTableMap().get(connectorNode.getTable());

                        FilterResult filterResult = filterResults(connectorNode, filter, targetTable);
                        TapAssert.asserts(() ->
                            assertNotNull(
                                filterResult,
                                TapSummary.format("exact.match.filter.null",InstanceFactory.instance(JsonParser.class).toJson(filterMap))
                            )
                        ).error(testCase);
                        if (null!=filterResult){
                            TapAssert.asserts(() -> Assertions.assertNull(
                                filterResult.getError(),
                                TapSummary.format("exact.match.filter.error",InstanceFactory.instance(JsonParser.class).toJson(filterMap),filterResult.getError())
                            )).error(testCase);
                            if (null==filterResult.getError()){
                                TapAssert.asserts(() -> assertNotNull(
                                    filterResult.getResult(),
                                    TapSummary.format("exact.match.filter.result.null")
                                )).error(testCase);
                                if (null!=filterResult.getResult()){
                                    Map<String, Object> result = filterResult.getResult();
                                    connectorNode.getCodecsFilterManager().transformToTapValueMap(result, targetTable.getNameFieldMap());
                                    connectorNode.getCodecsFilterManager().transformFromTapValueMap(result);
                                    StringBuilder builder = new StringBuilder();
                                    //boolean isEquals = mapEquals(record, result, builder);//精确匹配
                                    TapAssert.asserts(()->assertTrue(
                                        mapEquals(record, result, builder),
                                        TapSummary.format("exact.equals.failed",recordCount,builder.toString())
                                    )).acceptAsWarn(testCase,TapSummary.format("exact.equals.succeed",recordCount,builder.toString()));
//                                    if (isEquals){
//                                        TapAssert.succeed(testCase,TapSummary.format("exact.equals.succeed",recordCount));
//                                    }else {
//                                        //模糊匹配
//                                        boolean isMatch = objectIsEqual(record, result);
//                                        TapAssert.asserts(()->assertTrue(
//                                            isMatch,TapSummary.format("exact.match.failed",recordCount,builder.toString())
//                                        )).acceptAsWarn(testCase,TapSummary.format("exact.match.succeed",recordCount,builder.toString()));
//                                    }
                                }
                            }
                        }
                    }
                });
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }finally {
                if (hasCreateTable) execute.dropTable();
                super.connectorOnStop(prepare);
            }
        });
    }


    /**
     * 使用WriteRecordFunction插入3条， 使用BatchReadFunction， batchSize为2读出数据，
     * 返回数据条目数第一批应该为2， 第二批应该为1，
     * 如此返回的2批数据， 验证这2批数据和插入的3条数据保持顺序并且主键相同
     * */
    @DisplayName("clearTable.byBatch")//用例2，写入3条数据验证分批限制是有效的
    @TapTestCase(sort = 2)
    @Test
    void readAfterInsertByBatch(){
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            PDKTestBase.TestNode prepare = prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean hasCreatedTable = false;
            try {
                Method testCase = super.getMethod("readAfterInsertByBatch");
                super.connectorOnStart(prepare);
                execute.testCase(testCase);
                if (! (hasCreatedTable = super.createTable(prepare))){
                    return;
                }
                //使用WriteRecordFunction插入3条
                final int recordCount = 3;
                Record[] records = Record.testRecordWithTapTable(targetTable,recordCount);
                WriteListResult<TapRecordEvent> insert = execute.builderRecord(records).insert();
                TapAssert.asserts(()->
                    Assertions.assertTrue(
                        null!=insert && insert.getInsertedCount() == recordCount,
                        TapSummary.format("batchRead.insert.error",recordCount,null==insert?0:insert.getInsertedCount())
                    )
                ).acceptAsError(testCase, TapSummary.format("batchRead.insert.succeed",recordCount,null==insert?0:insert.getInsertedCount()));

                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnectorContext context = connectorNode.getConnectorContext();
                ConnectorFunctions functions = connectorNode.getConnectorFunctions();
                if (super.verifyFunctions(functions,testCase)){
                    return;
                }
                BatchReadFunction batchReadFun = functions.getBatchReadFunction();
                //使用BatchReadFunction， batchSize为2读出数据，
                final int batchSize = 2;
                List<List<TapEvent>> backData = new ArrayList<>();
                batchReadFun.batchRead(context,targetTable,null,batchSize,(list,consumer)->backData.add(list));
                //返回数据条目数第一批应该为2， 第二批应该为1，
                int times = recordCount/batchSize + (recordCount%batchSize>0?1:0);
                int tapEventIndex = 0;
                boolean isTrue = true;
                for (int index = 0; index < backData.size(); index++) {
                    List<TapEvent> tapEvents = backData.get(index);
                    int tapEventSize = null==tapEvents?0:tapEvents.size();
                    final int indexFinal = index;
                    if (times-1 != index){
                        if (null == tapEvents || tapEvents.size()!=batchSize){
                            //返回数据条目数第一批应该为2，
                            TapAssert.asserts(()->
                                Assertions.fail(TapSummary.format("batchRead.batchCount.error",recordCount,batchSize,indexFinal+1,batchSize,tapEventSize)
                            )).error(testCase);
                            break;
                        }else{
                            TapAssert.succeed(testCase,TapSummary.format("batchRead.batchCount.succeed",recordCount,batchSize,indexFinal+1,batchSize,tapEventSize));
                        }
                    }else {
                        //返回数据条目数第二批应该为1，
                        if (null == tapEvents || tapEvents.size() != (recordCount-batchSize*index) ){
                            TapAssert.asserts(()->
                                Assertions.fail(TapSummary.format("batchRead.batchCount.error",recordCount,batchSize,indexFinal+1,recordCount-batchSize*indexFinal,tapEventSize
                            ))).error(testCase);
                            break;
                        }else {
                            TapAssert.succeed(testCase,TapSummary.format("batchRead.batchCount.succeed",recordCount,batchSize,indexFinal+1,batchSize,tapEventSize));
                        }
                    }

                    for (int i = 0; i < tapEvents.size(); i++) {
                        if (isTrue){
                            TapEvent event = tapEvents.get(i);
                            if (null == event) {
                                isTrue = false;
                                continue;
                            }
                            Map<String, Object> after = ((TapInsertRecordEvent) event).getAfter();
                            if (null == after){
                                isTrue = false;
                                continue;
                            }
                            Record record = records[tapEventIndex];
                            isTrue =  null!=record && record.get("id").equals(after.get("id"));
                        }
                        tapEventIndex++;
                    }
                }
                if(tapEventIndex==recordCount){
                    //如此返回的2批数据， 验证这2批数据和插入的3条数据保持顺序并且主键相同
                    boolean finalIsTrue = isTrue;
                    TapAssert.asserts(()->
                        Assertions.assertTrue(finalIsTrue,TapSummary.format("batchRead.final.error",recordCount,recordCount))
                    ).acceptAsError(testCase,TapSummary.format("batchRead.final.succeed",recordCount,recordCount));
                }

            } catch (Throwable e) {
                throw new RuntimeException(e);
            }finally {
                if(hasCreatedTable) execute.dropTable();
                super.connectorOnStop(prepare);
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return list(
                support(WriteRecordFunction.class, TapSummary.format("WriteRecordFunctionNeed")),
                support(BatchReadFunction.class,TapSummary.format("BatchReadFunctionNeed"))
//                support(DropTableFunction.class, TapSummary.format("DropTableFunctionNeed"))
                //support(QueryByAdvanceFilterFunction.class, TapSummary.format("query_by_advance_filter_function_need"))
        );
    }
}
