package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
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
import java.util.Arrays;
import java.util.List;

import static io.tapdata.entity.simplify.TapSimplify.list;

@DisplayName("batchCountTest")//BatchCountFunction全量记录数（依赖WriteRecordFunction）
/**
 * 都需使用随机ID建表， 如果有DropTableFunction实现， 测试用例应该自动删除创建的临时表（无论成功或是失败）
 * */
@TapGo(sort = 13)
public class BatchCountTest extends PDKTestBase {

    @DisplayName("batchCountTest.afterInsert")//用例1， 插入数据查询记录数
    @TapTestCase(sort = 1)
    //使用WriteRecordFunction写入2条数据， 使用BatchCountFunction查询记录数， 返回2为正确
    @Test
    void batchCountAfterInsert(){
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean createTable = false;
            try {
                super.connectorOnStart(prepare);
                Method testCase = super.getMethod("batchCountAfterInsert");
                execute.testCase(testCase);
                //使用WriteRecordFunction写入2条数据
                Record[] records = Record.testRecordWithTapTable(targetTable,2);
                execute.builderRecord(records);
                WriteListResult<TapRecordEvent> insert = execute.insert();
                TapAssert.asserts(()->{
                    Assertions.assertTrue(
                            null!=insert&&insert.getInsertedCount()==records.length,
                            TapSummary.format("batchCountTest.insert.error",records.length,null==insert?0:insert.getInsertedCount())
                    );
                }).acceptAsError(testCase, TapSummary.format("batchCountTest.insert",records.length,insert.getInsertedCount()));

                //使用BatchCountFunction查询记录数， 返回2为正确
                if(createTable = (null!=insert && insert.getInsertedCount() == records.length) ){
                    ConnectorNode connectorNode = prepare.connectorNode();
                    ConnectorFunctions functions = connectorNode.getConnectorFunctions();
                    if (super.verifyFunctions(functions,testCase)){
                        return;
                    }
                    BatchCountFunction batchCount = functions.getBatchCountFunction();
                    long count = batchCount.count(connectorNode.getConnectorContext(), targetTable);
                    TapAssert.asserts(()->{
                        Assertions.assertEquals(records.length, count, TapSummary.format("batchCount.afterInsert.error", records.length, count));
                    }).acceptAsError(testCase,TapSummary.format("batchCount.afterInsert.succeed",records.length,count));
                }
            }catch (Throwable e) {
                throw new RuntimeException(e);
            }finally {
                if (createTable) execute.dropTable();
                super.connectorOnStop(prepare);
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return list(
//                support(DropTableFunction.class, TapSummary.format(inNeedFunFormat,"DropTableFunction")),
//                support(WriteRecordFunction.class,TapSummary.format(inNeedFunFormat,"WriteRecordFunction")),
//                support(BatchCountFunction.class,TapSummary.format(inNeedFunFormat,"BatchCountFunction"))
        );
    }
}
