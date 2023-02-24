package io.tapdata.pdk.tdd.tests.v3;

import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;
import io.tapdata.pdk.tdd.core.PDKTestBaseV2;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.core.base.TestNode;
import io.tapdata.pdk.tdd.tests.support.*;
import io.tapdata.pdk.tdd.tests.v2.RecordEventExecute;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * WriteRecord的事件需要顺序执行（依赖WriteRecordFunction和QueryByAdvanceFilter/QueryByFilter）
 * 部分数据源由开启大事务的功能（将事务数据先存入本地， 在整体提交），
 * TDD应该打开大事务功能，
 * 在nodeConfig里的配置。 例如Oracle就有这个场景
 */
@DisplayName("sequentialTest")
@TapGo(tag = "V3", sort = 13, debug = false)
public class SequentialWriteRecordTest extends PDKTestBaseV2 {
    public static List<SupportFunction> testFunctions() {
        return list(supportAny(
                langUtil.formatLang(anyOneFunFormat, "QueryByAdvanceFilterFunction,QueryByFilterFunction"),
                QueryByAdvanceFilterFunction.class, QueryByFilterFunction.class),
                support(WriteRecordFunction.class, LangUtil.format(inNeedFunFormat, "WriteRecordFunction"))
        );
    }

    private static final int recordCount = 1;
    private final Record[] records = Record.testRecordWithTapTable(super.targetTable, recordCount);
    /**
     * 用例1，相同主键的数据发生连续事件最后能被正常修改
     * 相同主键的数据， 发生新增， 修改1， 修改2， 修改3， 修改4事件， 最后的时候， 查询这条数据应该是修改4的内容
     */
    @DisplayName("sequentialTest.modify")
    @TapTestCase(sort = 1)
    @Test
    public void sequentialTestOfModify() throws NoSuchMethodException {
        AtomicBoolean hasCreatedTable = new AtomicBoolean(false);
        super.execTest("sequentialTestOfModify", (node, testCase) -> {
            if (!this.insertAndModify(node, hasCreatedTable)){
                return;
            }
            //查询数据，并校验
            List<Map<String, Object>> result = super.queryRecords(node, super.targetTable, this.records);
            if (result.size() != recordCount) {
                TapAssert.error(testCase, "插入了一条数据，但是实际返回多条。");
            } else {
                Map<String, Object> resultMap = result.get(0);
                StringBuilder builder = new StringBuilder();
                boolean equals = super.mapEquals(this.records[0], resultMap, builder);
                TapAssert.asserts(() -> {
                    Assertions.assertTrue(equals, "数据前后对比不一致，对比结果为：%s");
                }).acceptAsError(testCase, "数据前后对比一致，对比结果一致");
            }
        }, (node, testCase) -> {
            //删除表
            if (hasCreatedTable.get()) {
                RecordEventExecute execute = node.recordEventExecute();
                execute.dropTable();
            }
        });
    }

    /**
     * 用例2， 相同主键的数据发生连续事件最后能被删除掉
     * 相同主键的数据， 发生新增， 修改1， 修改2， 修改3， 修改4， 删除事件， 最后的时候， 查询这条数据应该是为空的
     */
    @DisplayName("sequentialTest.delete")
    @TapTestCase(sort = 2)
    @Test
    public void sequentialTestOfDelete() throws NoSuchMethodException {
        AtomicBoolean hasCreatedTable = new AtomicBoolean(false);
        super.execTest("sequentialTestOfDelete", (node, testCase) -> {
            if (!this.insertAndModify(node, hasCreatedTable)){
                return;
            }
            try {
                node.recordEventExecute().delete();
            } catch (Throwable e) {
                TapAssert.error(testCase, "删除数据抛出了一个异常，error: %s.");
            }
            //查询数据，并校验
            List<Map<String, Object>> result = super.queryRecords(node, super.targetTable, this.records);
            if (!result.isEmpty()) {
                TapAssert.error(testCase, "插入了一条数据，但是实际返回多条。");
            } else {
                TapAssert.succeed(testCase, "数据前后对比一致，对比结果一致");
            }
        }, (node, testCase) -> {
            //删除表
            if (hasCreatedTable.get()) {
                RecordEventExecute execute = node.recordEventExecute();
                execute.dropTable();
            }
        });
    }

    private boolean modifyRecord(RecordEventExecute execute) {
        Record.modifyRecordWithTapTable(super.targetTable, this.records, 2, false);
        execute.builderRecordCleanBefore(this.records);
        try {
            execute.update();
            return Boolean.TRUE;
        } catch (Throwable throwable) {
            TapAssert.error(execute.testCase(), "插入数据抛出了一个异常，error: %s.");
            return Boolean.FALSE;
        }
    }
    private boolean insertAndModify(TestNode node, AtomicBoolean hasCreatedTable){
        RecordEventExecute execute = node.recordEventExecute();
        hasCreatedTable.set(super.createTable(node));
        if (!hasCreatedTable.get()) {
            //@TODO 建表失败
            return false;
        }
        Method testCase = execute.testCase();
        //插入数据
        execute.builderRecord(this.records);
        try {
            execute.insert();
        } catch (Throwable e) {
            TapAssert.error(testCase, "插入数据抛出了一个异常，error: %s.");
        }
        //修改数据
        final int modifyCount = 4;
        for (int i = 0; i < modifyCount; i++) {
            if (!this.modifyRecord(execute)) {
                return false;
            }
        }
        return true;
    }
}
