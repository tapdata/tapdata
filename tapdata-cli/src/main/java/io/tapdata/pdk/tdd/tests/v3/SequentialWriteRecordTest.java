package io.tapdata.pdk.tdd.tests.v3;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.entity.WriteListResult;
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
import org.ehcache.shadow.org.terracotta.offheapstore.HashingMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.HashMap;
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
@TapGo(tag = "V3", sort = 13, debug = true)
public class SequentialWriteRecordTest extends PDKTestBaseV2 {
    {
        if (PDKTestBaseV2.testRunning) {
            System.out.println(langUtil.formatLang("sequentialTest.wait"));
        }
    }

    public static List<SupportFunction> testFunctions() {
        return list(supportAny(
                langUtil.formatLang(anyOneFunFormat, "QueryByAdvanceFilterFunction,QueryByFilterFunction"),
                QueryByAdvanceFilterFunction.class, QueryByFilterFunction.class),
                support(WriteRecordFunction.class, LangUtil.format(inNeedFunFormat, "WriteRecordFunction"))
        );
    }

    private static final int recordCount = 1;
    private final int modifyCount = 4;
    private final Record[] records = Record.testRecordWithTapTable(super.targetTable, recordCount);

    /**
     * 用例1，相同主键的数据发生连续事件最后能被正常修改
     * 相同主键的数据， 发生新增， 修改1， 修改2， 修改3， 修改4事件， 最后的时候， 查询这条数据应该是修改4的内容
     */
    @DisplayName("sequentialTest.modify")
    @TapTestCase(sort = 1)
    @Test
    public void sequentialTestOfModify() throws NoSuchMethodException {
        System.out.println(langUtil.formatLang("sequentialTest.modify.wait"));
        AtomicBoolean hasCreatedTable = new AtomicBoolean(false);
        super.execTest("sequentialTestOfModify", (node, testCase) -> {
            this.translation(node);
            Map<String, Object> recordBefore = new HashMap<>(records[0]);
            if (!this.insertAndModify(node, hasCreatedTable)) {
                return;
            }
            //查询数据，并校验
            List<Map<String, Object>> result = super.queryRecords(node, super.targetTable, this.records);
            final int filterCount = result.size();
            if (filterCount != recordCount) {
                TapAssert.error(testCase, langUtil.formatLang("sequentialTest.modify.query.fail", recordCount, modifyCount, filterCount));
            } else {
                Map<String, Object> resultMap = result.get(0);
                StringBuilder builder = new StringBuilder();
                boolean equals = super.mapEquals(recordBefore, resultMap, builder);
                TapAssert.asserts(() -> {
                    Assertions.assertTrue(equals, langUtil.formatLang("sequentialTest.modify.query.succeed", recordCount, modifyCount, builder.toString()));
                }).acceptAsError(testCase, langUtil.formatLang("sequentialTest.modify.query.notEquals", recordCount, modifyCount, builder.toString()));
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
            this.translation(node);
            if (!this.insertAndModify(node, hasCreatedTable)) {
                return;
            }
            try {
                WriteListResult<TapRecordEvent> delete = node.recordEventExecute().delete();
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

    private void translation(TestNode node) {
        //@TODO 打开大事物
        DataMap nodeConfig = node.connectorNode().getConnectorContext().getNodeConfig();
        nodeConfig.kv("", "");
    }

    private boolean modifyRecord(RecordEventExecute execute, int times) {
        Record.modifyRecordWithTapTable(super.targetTable, this.records, 2, false);
        execute.builderRecordCleanBefore(this.records);
        WriteListResult<TapRecordEvent> update;
        try {
            update = execute.update();
        } catch (Throwable throwable) {
            TapAssert.error(execute.testCase(), langUtil.formatLang("sequentialTest.modifyRecord.throw", recordCount, times, throwable.getMessage()));
            return Boolean.FALSE;
        }
        TapAssert.asserts(() ->
                Assertions.assertTrue(
                        null != update && update.getModifiedCount() == recordCount,
                        langUtil.formatLang("sequentialTest.modifyRecord.fail",
                                recordCount,
                                times,
                                null == update ? 0 : update.getInsertedCount(),
                                null == update ? 0 : update.getModifiedCount(),
                                null == update ? 0 : update.getRemovedCount())
                )
        ).acceptAsError(execute.testCase(), langUtil.formatLang("sequentialTest.modifyRecord",
                recordCount,
                times,
                null == update ? 0 : update.getInsertedCount(),
                null == update ? 0 : update.getModifiedCount(),
                null == update ? 0 : update.getRemovedCount()));
        return Boolean.TRUE;
    }

    private boolean insertAndModify(TestNode node, AtomicBoolean hasCreatedTable) {
        RecordEventExecute execute = node.recordEventExecute();
        hasCreatedTable.set(super.createTable(node));
        if (!hasCreatedTable.get()) {
            //@TODO 建表失败
            return false;
        }
        Method testCase = execute.testCase();
        //插入数据
        execute.builderRecord(this.records);
        WriteListResult<TapRecordEvent> insert;
        try {
            insert = execute.insert();
        } catch (Throwable e) {
            TapAssert.error(testCase, langUtil.formatLang("sequentialTest.insertRecord.throw", recordCount, e.getMessage()));
            return false;
        }
        TapAssert.asserts(() ->
                Assertions.assertTrue(
                        null != insert && insert.getInsertedCount() == recordCount,
                        langUtil.formatLang("sequentialTest.insertRecord.fail",
                                recordCount,
                                null == insert ? 0 : insert.getInsertedCount(),
                                null == insert ? 0 : insert.getModifiedCount(),
                                null == insert ? 0 : insert.getRemovedCount())
                )
        ).acceptAsError(testCase, langUtil.formatLang("sequentialTest.insertRecord",
                recordCount,
                null == insert ? 0 : insert.getInsertedCount(),
                null == insert ? 0 : insert.getModifiedCount(),
                null == insert ? 0 : insert.getRemovedCount()));
        //修改数据
        for (int i = 0; i < modifyCount; i++) {
            if (!this.modifyRecord(execute, i + 1)) {
                return false;
            }
        }
        return true;
    }
}
