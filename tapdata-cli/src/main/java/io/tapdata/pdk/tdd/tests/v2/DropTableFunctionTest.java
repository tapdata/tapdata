package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.apis.functions.connector.target.DropTableFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.tdd.core.PDKTestBase;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.core.base.TestNode;
import io.tapdata.pdk.tdd.tests.basic.RecordEventExecute;
import io.tapdata.pdk.tdd.tests.support.LangUtil;
import io.tapdata.pdk.tdd.tests.support.TapAssert;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static io.tapdata.entity.simplify.TapSimplify.list;

@DisplayName("dropTable")//DropTableFunction删除表（依赖CreateTableFunction或者WriteRecordFunction）
@TapGo(tag = "V2", sort = 110)
public class DropTableFunctionTest extends PDKTestBase {
    {
        if (PDKTestBase.testRunning) {
            System.out.println(LangUtil.format("dropTable.test.wait"));
        }
    }
    /**
     * 当数据源实现了CreateTableFunction，
     * 用此方法来创建表（随机表名）或者没有实现CreateTableFunction但是实现了WriteRecordFunction，
     * 写入一条数据到随机表名进行建表， 然后调用DropTableFunction来删除这个随机表，
     * 然后调用discoverSchema指定该随机表名来验证，
     * 表不存在了为正确， 如果还存在报警告。 （
     * 我们以后会希望最简化PDK API的时候， 可能会让用户手动建表， 所以不做报错处理）
     */
    @DisplayName("dropTable.test")//用例1， 删除表测试
    @TapTestCase(sort = 1)
    @Test
    void drop() {
        System.out.println(LangUtil.format("dropTable.case.wait"));
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            try {
                Method testCase = super.getMethod("drop");
                super.connectorOnStart(prepare);
                execute.testCase(testCase);

                //写入一条数据到随机表名进行建表，
                if (super.createTable(prepare)) {
                    //然后调用DropTableFunction来删除这个随机表，
                    execute.dropTable();

                    //然后调用discoverSchema指定该随机表名来验证，
                    ConnectorNode connectorNode = prepare.connectorNode();
                    TapConnector connector = connectorNode.getConnector();
                    String tableId = targetTable.getId();
                    connector.discoverSchema(connectorNode.getConnectorContext(), list(tableId), 1000, consumer -> {
                        //表不存在了为正确， 如果还存在报警告。
                        //我们以后会希望最简化PDK API的时候， 可能会让用户手动建表， 所以不做报错处理
                        TapAssert.asserts(() ->
                                Assertions.assertTrue(
                                        null == consumer || consumer.isEmpty(),
                                        LangUtil.format("dropTable.error", tableId)
                                )
                        ).acceptAsWarn(testCase, LangUtil.format("dropTable.succeed", tableId));
                    });
                }
            } catch (Throwable e) {
                throw new RuntimeException(e);
            } finally {
                super.connectorOnStop(prepare);
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return list(
                support(WriteRecordFunction.class, LangUtil.format(inNeedFunFormat, "WriteRecordFunction")),
                support(DropTableFunction.class, LangUtil.format(inNeedFunFormat, "DropTableFunction"))
        );
    }
}
