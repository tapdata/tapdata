package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.target.*;
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
import java.util.*;

import static io.tapdata.entity.simplify.TapSimplify.*;
import static io.tapdata.entity.utils.JavaTypesToTapTypes.JAVA_Long;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


@DisplayName("queryByAdvanced")//QueryByAdvancedFilterFunction 基于匹配字段高级查询（依赖WriteRecordFunction）
@TapGo(sort = 3)//都需使用随机ID建表， 如果有DropTableFunction实现， 测试用例应该自动删除创建的临时表（无论成功或是失败）
public class QueryByAdvancedFilterTest extends PDKTestBase {
    private static final String TAG = QueryByAdvancedFilterTest.class.getSimpleName();

    @Test
    @DisplayName("test.byAdvance.sourceTest")
    @TapTestCase(sort = 1)
    /**
     * 使用WriteRecordFunction插入一条全类型（覆盖TapType的11中类型数据）数据，通过主键作为匹配参数，
     * 查询出该条数据， 再进行精确和模糊值匹配， 只要能查出来数据就算是正确。
     * 如果值只能通过模糊匹配成功， 报警告指出是靠模糊匹配成功的，
     * 如果连模糊匹配也匹配不上， 也报警告（值对与不对很多时候不好判定）。
     * */
    void sourceTest() throws Throwable {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean hasCreateTable = false;
            try {
                Method testCase = super.getMethod("sourceTest");
                super.connectorOnStart(prepare);
                execute.testCase(testCase);

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
                hasCreateTable = null!=insert && insert.getInsertedCount() == recordCount;
                if (!hasCreateTable) return;
                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnectorContext context = connectorNode.getConnectorContext();
                ConnectorFunctions functions = connectorNode.getConnectorFunctions();
                if (super.verifyFunctions(functions,testCase)){
                    return;
                }
                QueryByAdvanceFilterFunction query = functions.getQueryByAdvanceFilterFunction();
                TapAdvanceFilter queryFilter = new TapAdvanceFilter();
                List<Map<String,Object>> consumer = filter(connectorNode,query,queryFilter);

                {
                    //数据条目数需要等于1， 查询出这1条数据，只要能查出来数据就算是正确。
                    TapAssert.asserts(()->
                            Assertions.assertTrue(
                                    null!=consumer&&consumer.size()==recordCount,
                                    TapSummary.format("byAdvance.query.error",recordCount,null==consumer?0:consumer.size())
                            )
                    ).acceptAsError(testCase,TapSummary.format("byAdvance.query.succeed",recordCount,null==consumer?0:consumer.size()));
                    if (null != consumer && consumer.size() == 1){
                        Record record = records[0];
                        Map<String,Object> tapEvent = consumer.get(0);
                        DataMap filterMap = new DataMap();
                        filterMap.putAll(tapEvent);
                        TapFilter filter = new TapFilter();
                        filter.setMatch(filterMap);
                        TapTable targetTable = connectorNode.getConnectorContext().getTableMap().get(connectorNode.getTable());

                        FilterResult filterResult = filterResults(connectorNode, filter, targetTable);
                        TapAssert.asserts(() ->
                                assertNotNull(
                                        filterResult,
                                        TapSummary.format("exact.match.filter.null", InstanceFactory.instance(JsonParser.class).toJson(filterMap))
                                )
                        ).acceptAsError(testCase,null);
                        if (null!=filterResult){
                            TapAssert.asserts(() -> Assertions.assertNull(
                                    filterResult.getError(),
                                    TapSummary.format("exact.match.filter.error",InstanceFactory.instance(JsonParser.class).toJson(filterMap),filterResult.getError())
                            )).acceptAsError(testCase,null);
                            if (null==filterResult.getError()){
                                TapAssert.asserts(() -> assertNotNull(
                                        filterResult.getResult(),
                                        TapSummary.format("exact.match.filter.result.null")
                                )).acceptAsError(testCase,null);
                                if (null!=filterResult.getResult()){
                                    Map<String, Object> result = filterResult.getResult();
                                    connectorNode.getCodecsFilterManager().transformToTapValueMap(result, targetTable.getNameFieldMap());
                                    connectorNode.getCodecsFilterManager().transformFromTapValueMap(result);
                                    StringBuilder builder = new StringBuilder();
                                    TapAssert.asserts(()->assertTrue(
                                            mapEquals(record, result, builder),TapSummary.format("exact.match.failed",recordCount,builder.toString())
                                    )).acceptAsWarn(testCase,TapSummary.format("exact.match.succeed",recordCount));
                                }
                            }
                        }
                    }
                }
//                BatchReadFunction batchReadFun = functions.getBatchReadFunction();
                //使用BatchReadFunction， batchSize为10读出所有数据，
                final int batchSize = 10;
//                batchReadFun.batchRead(context,targetTable,null,batchSize,(list,consumer)->);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }finally {
                if (hasCreateTable) {
                    execute.dropTable();
                }
                super.connectorOnStop(prepare);
            }
        });
        //waitCompleted(5000000);
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
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean hasCreateTable = false;
            try{
                Method testCase = this.getMethod("sourceTest2");
                execute.testCase(testCase);
                super.connectorOnStart(prepare);
                final int insertCount = 2;
                Record[] records = Record.testRecordWithTapTable(targetTable,insertCount);
                execute.builderRecord(records);
                WriteListResult<TapRecordEvent> insert = execute.insert();
                TapAssert.asserts(()->
                        Assertions.assertTrue(
                                null!=insert && insert.getInsertedCount() == insertCount,
                                TapSummary.format("batchRead.insert.error",insertCount,null==insert?0:insert.getInsertedCount())
                        )
                ).acceptAsError(testCase, TapSummary.format("batchRead.insert.succeed",insertCount,null==insert?0:insert.getInsertedCount()));
                hasCreateTable = null!=insert && insert.getInsertedCount() == insertCount;
                if (!hasCreateTable) return;
                ConnectorNode connectorNode = prepare.connectorNode();
                ConnectorFunctions functions = connectorNode.getConnectorFunctions();
                if (super.verifyFunctions(functions,testCase)){
                    return;
                }
                String tableId = targetTable.getId();
                QueryByAdvanceFilterFunction query = functions.getQueryByAdvanceFilterFunction();

                TapAdvanceFilter operatorFilter = new TapAdvanceFilter();
                String key = "id";
                Object value = records[0].get(key);
                operatorFilter.match(new DataMap().kv(key,value));
                List<Map<String, Object>> operator = filter(connectorNode, query, operatorFilter);
                TapAssert.asserts(()->{
                    Assertions.assertFalse(operator.isEmpty(),TapSummary.format("queryByAdvanced.operator.error",key,value,tableId));
                }).acceptAsError(testCase,TapSummary.format("queryByAdvanced.operator.succeed",key,value,tableId));

                TapAdvanceFilter sortFilter = new TapAdvanceFilter();
                sortFilter.sort(new SortOn(key,SortOn.ASCENDING));
                List<Map<String, Object>> sort = filter(connectorNode, query, sortFilter);
                TapAssert.asserts(()->{
                    Assertions.assertFalse(sort.isEmpty(),TapSummary.format("queryByAdvanced.sort.error",key,"ASCENDING",tableId));
                }).acceptAsError(testCase,TapSummary.format("queryByAdvanced.sort.succeed",key,"ASCENDING",tableId));

                TapAdvanceFilter projectionFilter = new TapAdvanceFilter();
                projectionFilter.projection(new Projection().include(key));
                List<Map<String, Object>> projection = filter(connectorNode, query, projectionFilter);
                TapAssert.asserts(()->{
                    Assertions.assertFalse(projection.isEmpty(),TapSummary.format("queryByAdvanced.projection.error",key,tableId));
                }).acceptAsError(testCase,TapSummary.format("queryByAdvanced.projection.succeed",key,tableId));
            }catch (Throwable e){
                throw new RuntimeException(e);
            }finally {
                if (hasCreateTable) execute.dropTable();
                super.connectorOnStop(prepare);
            }
        });
        //waitCompleted(5000000);
    }

    private List<Map<String,Object>> filter(ConnectorNode connectorNode,QueryByAdvanceFilterFunction query,TapAdvanceFilter operatorFilter) throws Throwable {
        List<Map<String, Object>> events = new ArrayList<>();
        query.query(
                connectorNode.getConnectorContext(),
                operatorFilter,targetTable,con->{
                    if (null!=con&&null!=con.getResults()&&!con.getResults().isEmpty()){
                        events.addAll(con.getResults());
                    }
                }
        );
        return events;
    }

    private void queryByAdvanceFilterTest(ConnectorNode targetNode,RecordEventExecute recordEventExecute) throws Throwable{
        ConnectorFunctions connectorFunctions = targetNode.getConnectorFunctions();
        QueryByAdvanceFilterFunction queryByAdvanceFilterFunction = connectorFunctions.getQueryByAdvanceFilterFunction();

        Record record = Record.create()
                .builder("id", 111111)
                .builder("name", "gavin")
                .builder("text", "gavin test");
        recordEventExecute.builderRecord(record);
        Method testCase = recordEventExecute.testCase();

        //插入一条记录，并获取查询结果对比插入前后是否一致
        recordEventExecute.insert();
        queryByAdvanceFilterFunction.query(
                targetNode.getConnectorContext(),
                TapAdvanceFilter.create().op(QueryOperator.lte("id", 111111)).op(QueryOperator.gte("id", 111111)),
                targetTable,
                filterResults -> $(() -> {
                    TapAssert.asserts(
                            ()->Assertions.assertNotNull(filterResults, TapSummary.format("WriteRecordWithQueryTest.sourceTest.insert.error.null"))//
                    ).acceptAsError(testCase,TapSummary.format("WriteRecordWithQueryTest.sourceTest.insert.succeed.notNull"));//

                    TapAssert.asserts(
                            ()->Assertions.assertNotNull(filterResults.getResults(), TapSummary.format("WriteRecordWithQueryTest.sourceTest.insert.error.nullResult"))//
                    ).acceptAsError(testCase,TapSummary.format("WriteRecordWithQueryTest.sourceTest.insert.succeed.notNullResult"));//

                    TapAssert.asserts(
                            ()-> Assertions.assertTrue(objectIsEqual(
                                filterResults.getResults(),
                                Collections.singletonList(record)),
                                TapSummary.format("WriteRecordWithQueryTest.sourceTest.insert.error.notEquals"))//
                    ).acceptAsWarn(testCase,TapSummary.format("WriteRecordWithQueryTest.sourceTest.insert.succeed.equals"));//
                })
        );

        //修改插入的记录，并对比插入前后的结果是否一致
        record.builder("name","Gavin pro").builder("text","Gavin pro max.");
        recordEventExecute.update();
        queryByAdvanceFilterFunction.query(
                targetNode.getConnectorContext(),
                TapAdvanceFilter.create().match(DataMap.create().kv("id", 111111).kv("name","Gavin pro").kv("text","Gavin pro max.")),
                targetTable,
                filterResults -> {
                    TapAssert.asserts(
                            ()->Assertions.assertNotNull(filterResults, TapSummary.format("WriteRecordWithQueryTest.sourceTest.update.error.null"))
                    ).acceptAsError(testCase,TapSummary.format("WriteRecordWithQueryTest.sourceTest.update.succeed.notNull"));

                    TapAssert.asserts(
                            ()->Assertions.assertNotNull(filterResults.getResults(), TapSummary.format("WriteRecordWithQueryTest.sourceTest.update.error.nullResult"))
                    ).acceptAsError(testCase,TapSummary.format("WriteRecordWithQueryTest.sourceTest.update.succeed.notNullResult"));

                    TapAssert.asserts(
                            ()-> Assertions.assertTrue(objectIsEqual(
                                    filterResults.getResults(),
                                    Collections.singletonList(record)),
                                    TapSummary.format("WriteRecordWithQueryTest.sourceTest.update.error.notEquals"))
                    ).acceptAsWarn(testCase,TapSummary.format("WriteRecordWithQueryTest.sourceTest.update.succeed.equals"));
                });

        //删除插入的记录，并检查删除是否成功
        recordEventExecute.delete();
        queryByAdvanceFilterFunction.query(
                targetNode.getConnectorContext(),
                TapAdvanceFilter.create().match(DataMap.create().kv("id", 111111).kv("name", "gavin").kv("text", "gavin test")),
                targetTable,
                filterResults -> {
                    TapAssert.asserts(
                            ()->Assertions.assertNotNull(filterResults, TapSummary.format("WriteRecordWithQueryTest.sourceTest.delete.error.null"))
                    ).acceptAsError(testCase,TapSummary.format("WriteRecordWithQueryTest.sourceTest.delete.succeed.notNull"));

                    TapAssert.asserts(
                            ()->Assertions.assertNull(filterResults.getResults(), TapSummary.format("WriteRecordWithQueryTest.sourceTest.delete.error.notNullResult"))
                    ).acceptAsError(testCase,TapSummary.format("WriteRecordWithQueryTest.sourceTest.delete.succeed.nullResult"));
                });
    }

    private void insertAfterInsertSomeKey(ConnectorNode targetNode,RecordEventExecute recordEventExecute) throws Throwable {
        Record[] records = Record.testStart(1);
        recordEventExecute.builderRecord(records);
        Method testCase = recordEventExecute.testCase();

        //recordEventExecute.createTable();
        WriteListResult<TapRecordEvent> insert = recordEventExecute.insert();

        TapAssert.asserts(()->{Assertions.assertTrue(false,"This is an warn Case");})
                .acceptAsWarn(testCase,"This is an warn case.");

        TapAssert.asserts(()->{Assertions.assertTrue(false,"This is error Case");})
                .acceptAsError(testCase,"This is succeed case.");

        for (Record record : records) {
            record.builder("name","Gavin pro").builder("text","Gavin pro max-modify");
        }

        final String insertPolicy = "dml_insert_policy";
        DataMap nodeConfig = targetNode.getConnectorContext().getNodeConfig();

        nodeConfig.kv(insertPolicy,"update_on_exists");
        WriteListResult<TapRecordEvent> insertAfter = recordEventExecute.insert();

        //插入已存在数据时， 在策略为update_on_exists时， 应该返回给引擎有插入成功的计数统计。
        TapAssert.asserts(
            ()->Assertions.assertNotEquals(
                    insert.getInsertedCount(),
                    insertAfter.getModifiedCount() + insertAfter.getInsertedCount(),
                    insertPolicy+" - update_on_exists | The first time you insert "+
                            insert.getInsertedCount()+" record, the second time you insert "+
                            insert.getInsertedCount()+" records of the same primary key, the echo result is inserted "+
                            insertAfter.getInsertedCount()+", and the second time you update "+
                            insertAfter.getModifiedCount()+" record. The operation fails because of inconsistencies.")
        ).acceptAsError(

                testCase,
                "As update_on_exists policy,you insert "
                        +insert.getInsertedCount()
                        +" records, and succeed "
                        +(insertAfter.getModifiedCount() + insertAfter.getInsertedCount())
                        + " records, including modifiedCount and insertedCount.");

        TapAssert.asserts(()->Assertions.assertNotEquals(
                insert.getInsertedCount(),
                insertAfter.getModifiedCount(),
                insertPolicy + " - update_on_exists | After inserting ten pieces of data, insert another record with the same primary key but different contents, and display the result. Insert " +
                        insertAfter.getInsertedCount() + ", insert another, and update " +
                        insertAfter.getModifiedCount() + ". Poor observability")).acceptAsWarn(testCase,"");

        //@TODO Wran
        Assertions.assertNotEquals(
                insert.getInsertedCount(),
                insertAfter.getModifiedCount(),
                insertPolicy + " - update_on_exists | After inserting ten pieces of data, insert another record with the same primary key but different contents, and display the result. Insert " +
                        insertAfter.getInsertedCount() + ", insert another, and update " +
                        insertAfter.getModifiedCount() + ". Poor observability");


        nodeConfig.kv(insertPolicy,"ignore_on_exists");
        WriteListResult<TapRecordEvent> insertAfter2 = recordEventExecute.insert();
        Assertions.assertFalse(
                0 == insertAfter2.getModifiedCount() && 0 == insertAfter2.getInsertedCount(),
                insertPolicy+" - ignore_on_exists | In node config ,your choises is xxx,so the update count must be zero,but not zero now");
    }

    private void updateNotExistRecord(ConnectorNode targetNode,RecordEventExecute recordEventExecute) throws Throwable {
        Record[] records = Record.testStart(10);
        recordEventExecute.builderRecord(records);
        Method testCase = recordEventExecute.testCase();
        //recordEventExecute.createTable();
        WriteListResult<TapRecordEvent> insert = recordEventExecute.insert();
        for (Record record : records) {
            record.builder("name","Gavin pro").builder("text","Gavin pro max-modify");
        }
        WriteListResult<TapRecordEvent> insertAfter = recordEventExecute.insert();

        Assertions.assertNotEquals(
                insert.getInsertedCount(),
                insertAfter.getModifiedCount() + insertAfter.getInsertedCount(),
                "The first time you insert "+
                        insert.getInsertedCount()+" record, the second time you insert "+
                        insert.getInsertedCount()+" records of the same primary key, the echo result is inserted "+
                        insertAfter.getInsertedCount()+", and the second time you update "+
                        insertAfter.getModifiedCount()+" record. The operation fails because of inconsistencies.");
        String insertPolic = "";

        if ("".equals(insertPolic)) {
            //@TODO Wran
            Assertions.assertNotEquals(
                    insert.getInsertedCount(),
                    insertAfter.getModifiedCount(),
                    "After inserting ten pieces of data, insert another record with the same primary key but different contents, and display the result. Insert " +
                            insertAfter.getInsertedCount() + ", insert another, and update " +
                            insertAfter.getModifiedCount() + ". Poor observability");
        }else if("".equals(insertPolic)){
            Assertions.assertNotEquals(
                    0,
                    insertAfter.getModifiedCount(),
                    "In node config ,your choises is xxx,so the update count must be zero,but not zero now");
        }
    }

    private void deleteNotExistRecord(ConnectorNode targetNode,RecordEventExecute recordEventExecute) throws Throwable {
        Record[] records = Record.testStart(10);
        recordEventExecute.builderRecord(records);
        Method testCase = recordEventExecute.testCase();

        //recordEventExecute.createTable();
        WriteListResult<TapRecordEvent> insert = recordEventExecute.insert();
        for (Record record : records) {
            record.builder("name","Gavin pro").builder("text","Gavin pro max-modify");
        }
        WriteListResult<TapRecordEvent> insertAfter = recordEventExecute.insert();

        Assertions.assertNotEquals(
                insert.getInsertedCount(),
                insertAfter.getModifiedCount() + insertAfter.getInsertedCount(),
                "The first time you insert "+
                        insert.getInsertedCount()+" record, the second time you insert "+
                        insert.getInsertedCount()+" records of the same primary key, the echo result is inserted "+
                        insertAfter.getInsertedCount()+", and the second time you update "+
                        insertAfter.getModifiedCount()+" record. The operation fails because of inconsistencies.");
        String insertPolic = "";

        if ("".equals(insertPolic)) {
            //@TODO Wran
            Assertions.assertNotEquals(
                    insert.getInsertedCount(),
                    insertAfter.getModifiedCount(),
                    "After inserting ten pieces of data, insert another record with the same primary key but different contents, and display the result. Insert " +
                            insertAfter.getInsertedCount() + ", insert another, and update " +
                            insertAfter.getModifiedCount() + ". Poor observability");
        }else if("".equals(insertPolic)){
            Assertions.assertNotEquals(
                    0,
                    insertAfter.getModifiedCount(),
                    "In node config ,your choises is xxx,so the update count must be zero,but not zero now");
        }
    }

    public static List<SupportFunction> testFunctions() {
        return list(
                support(WriteRecordFunction.class, TapSummary.format(inNeedFunFormat,"WriteRecordFunction")),
                support(QueryByAdvanceFilterFunction.class, TapSummary.format(inNeedFunFormat,"QueryByAdvanceFilterFunction")),
//                support(CreateTableFunction.class,"Create table is must to verify ,please implement CreateTableFunction in registerCapabilities method."),
                support(DropTableFunction.class,TapSummary.format(inNeedFunFormat,"DropTableFunction"))
        );
    }

}
