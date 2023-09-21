package io.tapdata.pdk.tdd.tests.v2;

import io.tapdata.pdk.apis.context.TapConnectorContext;
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

@DisplayName("tableCount.test")//tableCount表数量， 必测方法
@TapGo(tag = "V2", sort = 50)
public class TableCountTest extends PDKTestBase {
    {
        if (PDKTestBase.testRunning) {
            System.out.println(LangUtil.format("tableCount.test.wait"));
        }
    }
    @DisplayName("tableCount.findTableCount")//用例1， 查询表数量
    @Test
    @TapTestCase(sort = 1)
    /**
     * 调用tableCount方法之后返回表数量大于1为正确
     * */
    void findTableCount() {
        System.out.println(LangUtil.format("tableCount.findTableCount.wait"));
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            //setTable();
            try {
                super.connectorOnStart(prepare);
                prepare.recordEventExecute().testCase(super.getMethod("findTableCount"));
                tableCount(prepare);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            } finally {
                super.connectorOnStop(prepare);
            }
        });
    }

    int tableCount(TestNode prepare) throws Throwable {
        TapConnectorContext connectorContext = prepare.connectorNode().getConnectorContext();
        Method testCase = prepare.recordEventExecute().testCase();
        int tableCount = 0;
        try {
            tableCount = prepare.connectorNode().getConnector().tableCount(connectorContext);
        } catch (Exception e) {
            TapAssert.asserts(() ->
                    Assertions.fail(LangUtil.format("tableCount.findTableCount.errorFun", e.getMessage()))
            ).acceptAsError(testCase, null);
        }
        int tableCountFinal = tableCount;
        TapAssert.asserts(() ->
                Assertions.assertTrue(tableCountFinal > 0,
                        LangUtil.format("tableCount.findTableCount.error", tableCountFinal)
                )
        ).acceptAsError(testCase,
                LangUtil.format("tableCount.findTableCount.succeed", tableCountFinal)
        );
        return tableCount;
    }

    @DisplayName("tableCount.findTableCountAfterNewTable")//用例2， 新建表之后查询表数量
    @Test
    @TapTestCase(sort = 2)
    /**
     * 调用tableCount方法之后获得表数量，
     * 调用CreateTableFunction新建一张表，
     * 再调用tableCount方法获得表数量，
     * 比之前的数量加1就是正确的
     * */
    void findTableCountAfterNewTable() {
        System.out.println(LangUtil.format("tableCount.findTableCountAfterNewTable.wait"));
        super.consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = this.prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            boolean hasCreatedTable = false;
            try {
                super.connectorOnStart(prepare);
                Method testCase = super.getMethod("findTableCountAfterNewTable");
                execute.testCase(testCase);

                //调用tableCount方法之后获得表数量，
                int tableCountNewTableAgo = tableCount(prepare);

                //调用CreateTableFunction新建一张表，
                if (!(hasCreatedTable = this.createTable(prepare))) return;

                //再调用tableCount方法获得表数量，
                int tableCountNewTableAfter = tableCount(prepare);

                //比之前的数量加1就是正确的 | 实际场景下不一定数量高度符合预期
                TapAssert.asserts(() ->
                        Assertions.assertEquals(
                                tableCountNewTableAfter,
                                tableCountNewTableAgo + 1,
                                LangUtil.format(
                                        "tableCount.findTableCountAfterNewTable.afterNewTable.error",
                                        tableCountNewTableAgo,
                                        tableCountNewTableAfter
                                )
                        )
                ).acceptAsWarn(
                        testCase,
                        LangUtil.format(
                                "tableCount.findTableCountAfterNewTable.afterNewTable.succeed",
                                tableCountNewTableAgo,
                                tableCountNewTableAfter
                        )
                );
            } catch (Throwable e) {
                throw new RuntimeException(e);
            } finally {
                if (hasCreatedTable) execute.dropTable();
                super.connectorOnStop(prepare);
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return list(
//                support(DropTableFunction.class, LangUtil.format(inNeedFunFormat,"DropTableFunction"))
        );
    }
}
