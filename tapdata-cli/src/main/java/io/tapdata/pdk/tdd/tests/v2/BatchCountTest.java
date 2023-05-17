package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchCountFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.PDKTestBaseV2;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.core.base.StreamStopException;
import io.tapdata.pdk.tdd.core.base.TapAssertException;
import io.tapdata.pdk.tdd.core.base.TestNode;
import io.tapdata.pdk.tdd.tests.basic.RecordEventExecute;
import io.tapdata.pdk.tdd.tests.support.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static io.tapdata.entity.simplify.TapSimplify.list;

@DisplayName("batchCountTest")//BatchCountFunction全量记录数（依赖WriteRecordFunction）
/**
 * 都需使用随机ID建表， 如果有DropTableFunction实现， 测试用例应该自动删除创建的临时表（无论成功或是失败）
 * */
@TapGo(tag = "V2", sort = 130,debug = false)
public class BatchCountTest extends PDKTestBase {
    {
        if (PDKTestBase.testRunning) {
            System.out.println(LangUtil.format("batchCountTest.wait"));
        }
    }
    @DisplayName("batchCountTest.afterInsert")//用例1， 插入数据查询记录数
    @TapTestCase(sort = 1)
    //使用WriteRecordFunction写入2条数据， 使用BatchCountFunction查询记录数， 返回2为正确
    @Test
    void batchCountAfterInsert() throws NoSuchMethodException {
        System.out.println(LangUtil.format("batchCountTest.afterInsert.wait"));
        Method testCase = super.getMethod("batchCountAfterInsert");
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean createTable = false;
            try {
                super.connectorOnStart(prepare);
                execute.testCase(testCase);
                if (!(createTable = super.createTable(prepare))) {
                    return;
                }
                //使用WriteRecordFunction写入2条数据
                Record[] records = Record.testRecordWithTapTable(targetTable, 2);
                execute.builderRecord(records);
                WriteListResult<TapRecordEvent> insert = execute.insert();
                TapAssert.asserts(() -> {
                    Assertions.assertTrue(
                            null != insert && insert.getInsertedCount() == records.length,
                            LangUtil.format("batchCountTest.insert.error", records.length, null == insert ? 0 : insert.getInsertedCount())
                    );
                }).acceptAsError(testCase, LangUtil.format("batchCountTest.insert", records.length, insert.getInsertedCount()));
                //使用BatchCountFunction查询记录数， 返回2为正确
                if (createTable = (insert.getInsertedCount() == records.length)) {
                    TapTable targetTableModel = super.getTargetTable(prepare.connectorNode());
                    ConnectorNode connectorNode = prepare.connectorNode();
                    ConnectorFunctions functions = connectorNode.getConnectorFunctions();
                    if (super.verifyFunctions(functions, testCase)) {
                        return;
                    }
                    BatchCountFunction batchCount = functions.getBatchCountFunction();
                    long count = batchCount.count(connectorNode.getConnectorContext(), targetTableModel);
                    TapAssert.asserts(() -> {
                        Assertions.assertEquals(records.length, count, LangUtil.format("batchCount.afterInsert.error", records.length, count));
                    }).acceptAsError(testCase, LangUtil.format("batchCount.afterInsert.succeed", records.length, count));
                }
            } catch (Throwable e) {
                if ( !(e instanceof TapAssertException) && !(e instanceof StreamStopException)) {
                    TapAssert.error(testCase, LangUtil.format("fieldModification.all.throw", e.getMessage()));
                } else {
                    e.getCause();
                }
            } finally {
                if (createTable) execute.dropTable();
                super.connectorOnStop(prepare);
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return list();
    }
}
