package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.target.DropTableFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.cli.commands.TapSummary;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.api.PDKIntegration;
import io.tapdata.pdk.core.monitor.PDKInvocationMonitor;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.core.workflow.engine.DataFlowWorker;
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


@DisplayName("Test.WriteRecordTest")
@TapGo(sort = 1)
/**
 * 都需使用随机ID建表， 如果有DropTableFunction实现， 测试用例应该自动删除创建的临时表（无论成功或是失败）
 * */
public class WriteRecordTest extends PDKTestBase {
    private static final String TAG = WriteRecordTest.class.getSimpleName();
    protected ConnectorNode tddTargetNode;
    protected ConnectorNode sourceNode;
    protected DataFlowWorker dataFlowWorker;
    protected String targetNodeId = "t2";
    protected String testSourceNodeId = "ts1";
    protected String originToSourceId;
    protected TapNodeInfo tapNodeInfo;
    protected String testTableId;
    protected TapTable targetTable = table(testTableId)
            .add(field("id", JAVA_Long).isPrimaryKey(true).primaryKeyPos(1))
            .add(field("name", "STRING"))
            .add(field("text", "STRING"));
    @Test
    @DisplayName("Test.WriteRecordTest.case.sourceTest1")//增删改数量返回正确
    @TapTestCase(sort = 1)
    /**
     * 插入2条数据， 修改插入的2条数据， 删除插入的2条数据 ，验证插入的数量， 修改的数量， 删除的数量是否正确。
     * */
    void sourceTest1() throws Throwable {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = prepare(nodeInfo);
            try {
                super.connectorOnStart(prepare);
                writeRecorde(prepare.recordEventExecute().testCase(this.getMethod("sourceTest1")));
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }finally {
                prepare.recordEventExecute().dropTable();
                super.connectorOnStop(prepare);
            }
        });
        //waitCompleted(5000000);
    }
    long insertRecordNeed = 2;
    private void writeRecorde(RecordEventExecute recordEventExecute) throws Throwable {
       Record[] records = Record.testStart((int)insertRecordNeed);
       int recLen = records.length;
       recordEventExecute.builderRecord(records);
        Method testCase = recordEventExecute.testCase();

        WriteListResult<TapRecordEvent> insert = recordEventExecute.insert();
        long insertRecord = insert.getInsertedCount();
        TapAssert.asserts(()->
                Assertions.assertEquals(
                        recLen, insertRecord,
                        TapSummary.format("RecordEventExecute.insert.assert.error", recLen))
        ).acceptAsWarn(testCase,TapSummary.format("RecordEventExecute.insert.assert.succeed",recLen) );

       for (Record record : records) {
           record.builder("name","Gavin pro").builder("text","Gavin pro max-modify");
       }
       WriteListResult<TapRecordEvent> update = recordEventExecute.update();
       long updateRecord = update.getModifiedCount();
       TapAssert.asserts(()->Assertions.assertEquals(
               recLen, updateRecord,
               TapSummary.format("RecordEventExecute.update.assert.error",recLen))
       ).acceptAsError(testCase,TapSummary.format("RecordEventExecute.update.assert.succeed",recLen));


       WriteListResult<TapRecordEvent> delete = recordEventExecute.delete();
       long deleteRecord = delete.getRemovedCount();
       TapAssert.asserts(()->Assertions.assertEquals(
               recLen, deleteRecord,
               TapSummary.format("RecordEventExecute.delete.assert.error",recLen))
       ).acceptAsError(testCase,TapSummary.format("RecordEventExecute.delete.assert.succeed",recLen));

    }

    @Test
    @DisplayName("Test.WriteRecordTest.case.sourceTest2")// 多次插入相同主键的数据， 插入修改数量应该正确
    @TapTestCase(sort = 2)
    /**
     * 支持默认行为就是合格的， 默认以外的按警告处理
     * 插入2条数据， 再次插入相同主键的2条数据， 内容略有不同， 插入策略是update_on_exists（默认行为），
     *      此时验证新插入应该是插入2个， 后再插入的相同主键的2条数据应该是修改2个，
     *      假如是插入2个就应该是一个警告， 代表可观测性数据可能不准确。如果是其他情况都是错误的。
     * 插入2条数据， 再次插入相同主键的2条数据， 内容略有不同，
     *      插入策略是ignore_on_exists， 此时验证新插入应该是插入2个，
     *      后再插入的相同主键的2条数据应该是新增， 修改， 删除都没有数量。
     *      由于这个不是默认策略， 因此此处的错误都按警告处理并且提示用当策略是ignore_on_exists时， 需要怎么做。
     * */
    void sourceTest2() throws Throwable {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = prepare(nodeInfo);
            try {
                super.connectorOnStart(prepare);
                sourceTest2Fun(
                        prepare.recordEventExecute().testCase(this.getMethod("sourceTest2")),
                        prepare.connectorNode()
                );
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }finally {
                prepare.recordEventExecute().dropTable();
                super.connectorOnStop(prepare);
            }
        });
        //waitCompleted(5000000);
    }
    private void sourceTest2Fun( RecordEventExecute recordEventExecute,ConnectorNode connectorNode) throws Throwable {
        Record[] records = Record.testStart((int)insertRecordNeed);
        final int recLen = records.length;
        recordEventExecute.builderRecord(records);
        Method testCase = recordEventExecute.testCase();
        //插入2条数据， 再次插入相同主键的2条数据， 内容略有不同， 插入策略是update_on_exists（默认行为），
        WriteListResult<TapRecordEvent> insertBefore = recordEventExecute.insert();
        long firstInsert = insertBefore.getInsertedCount();
        //此时验证新插入应该是插入2个
        String firstInsertMsgError = TapSummary.format("WriteRecordTest.sourceTest2.verify.firstInsert", recLen, firstInsert);
        String firstInsertMsgSucceed = TapSummary.format("WriteRecordTest.sourceTest2.verify.firstInsert.succeed", recLen, firstInsert);
        TapAssert.asserts(()-> Assertions.assertEquals(recLen, firstInsert,firstInsertMsgError ))
                .acceptAsError(testCase,firstInsertMsgSucceed);

        for (int index = 0; index < insertRecordNeed; index++) {
            records[index].builder("name","yes please update_on_exists.");
        }
        WriteListResult<TapRecordEvent> insertAfter = recordEventExecute.insert();
        long lastInsert = insertAfter.getInsertedCount();
        long lastUpdate = insertAfter.getModifiedCount();

        //新增和修改之和等于操作数，否则失败
        //插入2条数据， 再次插入相同主键的2条数据， 内容略有不同，
        final String suffix = "WriteRecordTest.sourceTest2.verify.insertAfter.";
        TapAssert asserts = TapAssert.asserts(() -> Assertions.assertTrue(
                lastUpdate+lastInsert==recLen
                        && lastUpdate == recLen
                        && lastInsert == 0 ,
                lastUpdate+lastInsert!=recLen?
                        TapSummary.format(suffix+"NotEquals",recLen,recLen,recLen): lastInsert == recLen && lastUpdate == 0?
                        TapSummary.format(suffix+"WarnInsert",lastInsert):lastUpdate != recLen?
                        TapSummary.format(suffix+"WarnUpdate",lastUpdate,recLen,recLen)
                        :TapSummary.format(suffix+"ErrorOther",lastUpdate,lastInsert,recLen))
        );
        String succeed = TapSummary.format(suffix+"Succeed",recLen,lastUpdate,lastInsert);
        if (lastUpdate == recLen && lastUpdate+lastInsert==recLen) {
            //后再插入的相同主键的2条数据应该是修改2个
            asserts.acceptAsWarn(testCase,succeed);
        }else if(lastInsert == recLen && lastUpdate+lastInsert==recLen){
            //假如是插入2个就应该是一个警告， 代表可观测性数据可能不准确。
            asserts.acceptAsWarn(testCase,succeed);
        }else {
            //如果是其他情况都是错误的。
            asserts.acceptAsError(testCase,succeed);
        }


//            final String insertPolicy = "dml_insert_policy";
//            ConnectorCapabilities connectorCapabilities = connectorNode.getConnectorContext().getConnectorCapabilities();
//            Map<String, String> capabilityAlternativeMap = connectorCapabilities.getCapabilityAlternativeMap();
//            if (null == capabilityAlternativeMap){
//                capabilityAlternativeMap = new HashMap<>();
//                connectorCapabilities.setCapabilityAlternativeMap(capabilityAlternativeMap);
//            }
//            capabilityAlternativeMap.put(insertPolicy,"ignore_on_exists");
        super.ignoreOnExistsWhenInsert(connectorNode.getConnectorContext());
        //插入策略是ignore_on_exists
        //插入2条数据， 再次插入相同主键的2条数据， 内容略有不同， 插入策略是ignore_on_exists，
        for (int index = 0; index < insertRecordNeed; index++) {
            records[index].builder("name","yes please ignore_on_exists.");
        }

        WriteListResult<TapRecordEvent> insertAfter2 = recordEventExecute.insert();
        long lastInsert2 = insertAfter2.getInsertedCount();
        long lastUpdate2 = insertAfter2.getModifiedCount();

        //新增和修改之和等于操作数，否则失败
        //插入2条数据， 再次插入相同主键的2条数据， 内容略有不同，
        final String suffix2 = "WriteRecordTest.sourceTest2.IOE.verify.insertAfter.";
        TapAssert.asserts(() -> Assertions.assertTrue(
                lastUpdate2 == 0 && lastInsert2 == 0 ,
                        TapSummary.format(suffix2+"error",lastInsert2,lastUpdate2))
        ).acceptAsWarn(testCase,TapSummary.format(suffix2+"succeed",lastInsert2,lastUpdate2));
    }

    @Test
    @DisplayName("Test.WriteRecordTest.case.sourceTest3")// 删除不存在的数据时，删除数量应该正确
    @TapTestCase(sort = 3)
    /**
     * 删除1条不存在的数据， 此时不应该报错， 且返回给引擎的插入， 修改和删除都应该为0.
     * */
    void sourceTest3() throws Throwable {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = prepare(nodeInfo);
            try {
                super.connectorOnStart(prepare);
                sourceTest3Fun(
                        prepare.recordEventExecute().testCase(this.getMethod("sourceTest3")),
                        prepare.connectorNode()
                );
            } catch (Throwable e) {
                throw new RuntimeException(e);
            } finally {
                prepare.recordEventExecute().dropTable();
                super.connectorOnStop(prepare);
            }
        });
        //waitCompleted(5000000);
    }
    void sourceTest3Fun( RecordEventExecute recordEventExecute,ConnectorNode connectorNode){
        Record[] records = Record.testStart(1);
        final int recLen = records.length;
        recordEventExecute.builderRecord(records);
        try {
            WriteListResult<TapRecordEvent> delete = recordEventExecute.delete();
            TapAssert.asserts(()->Assertions.assertTrue(
                    null != delete &&
                            delete.getRemovedCount() ==0 &&
                            delete.getModifiedCount() ==0 &&
                            delete.getInsertedCount() ==0,
                    TapSummary.format("writeRecordTest.sourceTest3.deleteNotExist.error",
                            recLen,
                            delete.getInsertedCount(),
                            delete.getModifiedCount(),
                            delete.getRemovedCount()
                    ))
            ).acceptAsWarn(
                     recordEventExecute.testCase(),
                    TapSummary.format("writeRecordTest.sourceTest3.deleteNotExist.succeed",recLen)
            );
        }catch (Throwable throwable) {
            TapAssert.asserts(()->Assertions.assertDoesNotThrow(
                    recordEventExecute::delete,
                    TapSummary.format("writeRecordTest.sourceTest3.deleteNotExist.catchThrowable",recLen))
            ).acceptAsError(
                     recordEventExecute.testCase(),
                    TapSummary.format("writeRecordTest.sourceTest3.deleteNotExist.notThrowable",recLen)
            );
        }
    }

    @Test
    @DisplayName("Test.WriteRecordTest.case.sourceTest4")//修改不存在的数据， 插入修改数量应该正确
    @TapTestCase(sort = 4)
    /**
     * 修改1条不存在的数据， 如果修改策略是insert_on_nonexists， 此时验证新插入应该是1个
     * 修改1条不存在的数据， 如果修改策略是 ignore_on_nonexists， 此时验证插入和修改都应该为0个
     * */
    void sourceTest4() throws Throwable {
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = prepare(nodeInfo);
            try {
                super.connectorOnStart(prepare);
                sourceTest4Fun(
                        prepare.recordEventExecute().testCase(this.getMethod("sourceTest4")),
                        prepare.connectorNode()
                );
            } catch (Throwable e) {
                throw new RuntimeException(e);
            } finally {
                prepare.recordEventExecute().dropTable();
                super.connectorOnStop(prepare);
            }
        });
        //waitCompleted(5000000);
    }
    void sourceTest4Fun(RecordEventExecute recordEventExecute, ConnectorNode connectorNode){
        Method testCase = recordEventExecute.testCase();
        insertOnNotExists(recordEventExecute,connectorNode,testCase);
        ignoreOnNotExists(recordEventExecute,connectorNode,testCase);
    }
    private void insertOnNotExists(RecordEventExecute recordEventExecute, ConnectorNode connectorNode,Method testCase){
        Record[] records = Record.testStart(1);
        final int recLen = records.length;
        recordEventExecute.builderRecord(records);
        //修改1条不存在的数据， 如果修改策略是insert_on_nonexists， 此时验证新插入应该是1个
        super.insertOnExistsWhenUpdate(connectorNode.getConnectorContext());
        WriteListResult<TapRecordEvent> update1 = null;
        try {
            update1 = recordEventExecute.update();
        } catch (Throwable throwable) {
            TapAssert.asserts(()-> Assertions.fail(TapSummary.format("writeRecordTest.sourceTest4.insertOnNotExists.throwable", throwable.getMessage()))).acceptAsError(testCase, null);
            return;
        }
        WriteListResult<TapRecordEvent> updateFinal1 = update1;
        TapAssert.asserts(()->
                Assertions.assertTrue(
                        null != updateFinal1 && updateFinal1.getInsertedCount() == recLen,
                        TapSummary.format("writeRecordTest.sourceTest4.insertOnNotExists.error",recLen,recLen,updateFinal1.getInsertedCount()))
        ).acceptAsWarn(
                testCase,
                TapSummary.format("writeRecordTest.sourceTest4.insertOnNotExists.succeed",recLen,recLen,updateFinal1.getInsertedCount())
        );
    }
    private void ignoreOnNotExists(RecordEventExecute recordEventExecute, ConnectorNode connectorNode,Method testCase){
        Record[] records = Record.testStart(1);
        final int recLen2 = records.length;
        recordEventExecute.builderRecord(records);
        //修改1条不存在的数据， 如果修改策略是 ignore_on_nonexists， 此时验证插入和修改都应该为0个
        super.ignoreOnExistsWhenUpdate(connectorNode.getConnectorContext());
        WriteListResult<TapRecordEvent> update2 = null;
        try {
            update2 = recordEventExecute.update();
        }catch (Throwable throwable){
            TapAssert.asserts(()-> Assertions.fail(TapSummary.format("writeRecordTest.sourceTest4.ignoreOnNotExists.throwable", throwable.getMessage()))).acceptAsError(testCase, null);
            return;
        }
        final WriteListResult<TapRecordEvent> updateFinal2 = update2;
        TapAssert.asserts(()->
                Assertions.assertTrue(
                        null != updateFinal2 && updateFinal2.getInsertedCount() == 0 && updateFinal2.getModifiedCount() == 0,
                        TapSummary.format("writeRecordTest.sourceTest4.ignoreOnNotExists.error",recLen2,updateFinal2.getInsertedCount(),updateFinal2.getModifiedCount()))
        ).acceptAsWarn(
                testCase,
                TapSummary.format("writeRecordTest.sourceTest4.ignoreOnNotExists.succeed",recLen2,update2.getInsertedCount(),update2.getModifiedCount())
        );
    }

    public static List<SupportFunction> testFunctions() {
        return list(
                support(WriteRecordFunction.class, TapSummary.format(inNeedFunFormat,"WriteRecordFunction")),
//                support(CreateTableFunction.class,"Create table is must to verify ,please implement CreateTableFunction in registerCapabilities method."),
                //support(QueryByAdvanceFilterFunction.class, "QueryByAdvanceFilterFunction is a must for database which is schema free to sample some record to generate the field data types.")
                support(DropTableFunction.class, TapSummary.format(inNeedFunFormat,"DropTableFunction"))
        );
    }
}
