package io.tapdata.pdk.tdd.tests.v3;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.tdd.core.PDKTestBaseV2;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.core.base.TestNode;
import io.tapdata.pdk.tdd.tests.basic.RecordEventExecute;
import io.tapdata.pdk.tdd.tests.support.*;
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
@TapGo(tag = "V3", sort = 10020)
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
        super.execTest((node, testCase) -> {
            super.translation(node);
            //Map<String, Object> recordBefore = new HashMap<>(records[0]);
            if (!this.insertAndModify(node, hasCreatedTable)) {
                return;
            }
            //查询数据，并校验
            Record[] recordCopy = node.recordEventExecute().records();
            TapTable targetTableModel = super.getTargetTable(node.connectorNode());
            List<Map<String, Object>> result = super.queryRecords(node, targetTableModel, recordCopy);
            final int filterCount = result.size();
            if (filterCount != recordCount) {
                TapAssert.error(testCase, langUtil.formatLang("sequentialTest.modify.query.fail", recordCount, modifyCount, filterCount));
            } else {
                Map<String, Object> resultMap = result.get(0);
                StringBuilder builder = new StringBuilder();
                //node.connectorNode().getCodecsFilterManager().transformToTapValueMap(resultMap, targetTableModel.getNameFieldMap());
                //TapCodecsFilterManager.create(TapCodecsRegistry.create()).transformFromTapValueMap(resultMap);

                boolean equals = super.mapEquals(transform(node, targetTableModel, recordCopy[0]), resultMap, builder, targetTableModel.getNameFieldMap());
                TapAssert.asserts(() -> {
                    Assertions.assertTrue(equals, langUtil.formatLang("sequentialTest.modify.query.notEquals", recordCount, modifyCount, builder.toString()));
                }).acceptAsWarn(testCase, langUtil.formatLang("sequentialTest.modify.query.succeed", recordCount, modifyCount));
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
        System.out.println(langUtil.formatLang("sequentialTest.delete.wait"));
        AtomicBoolean hasCreatedTable = new AtomicBoolean(false);
        super.execTest((node, testCase) -> {
            super.translation(node);
            if (!this.insertAndModify(node, hasCreatedTable)) {
                return;
            }
            if (this.deleteRecord(node, modifyCount)) {
                return;
            }
            //查询数据，并校验
            Record[] recordCopy = node.recordEventExecute().records();
            TapTable targetTableModel = super.getTargetTable(node.connectorNode());
            List<Map<String, Object>> result = super.queryRecords(node, targetTableModel, recordCopy);
            TapAssert.asserts(() -> Assertions.assertTrue(result.isEmpty(), langUtil.formatLang("sequentialTest.delete.fail", recordCount, modifyCount, result.size())))
                    .acceptAsError(testCase, langUtil.formatLang("sequentialTest.delete.succeed", recordCount, modifyCount));
        }, (node, testCase) -> {
            //删除表
            if (hasCreatedTable.get()) {
                RecordEventExecute execute = node.recordEventExecute();
                execute.dropTable();
            }
        });
    }

    /**
     * 用例3， 相同主键的数据发生连续删除新增事件最后数据是正确的
     * 相同主键的数据，
     * 发生新增1， 修改1， 删除1， 新增2， 删除2， 新增3事件，
     * 最后的时候， 查询这条数据应该是为新增3的数据
     */
    @DisplayName("sequentialTest.more")
    @TapTestCase(sort = 3)
    @Test
    public void sequentialTestOfDeleteAndMore() throws NoSuchMethodException {
        System.out.println(langUtil.formatLang("sequentialTest.more.wait"));
        AtomicBoolean hasCreatedTable = new AtomicBoolean(false);
        super.execTest((node, testCase) -> {
            super.translation(node);
            final int mCount = 1;
            if (!this.insertAndModify(node, hasCreatedTable, true, mCount)) {
                return;
            }
            if (!this.deleteRecord(node, mCount)) {
                return;
            }
            if (!this.insertAndModify(node, hasCreatedTable, false, 0)) {
                return;
            }
            if (!this.deleteRecord(node, 0)) {
                return;
            }
            if (!this.insertAndModify(node, hasCreatedTable, false, 0)) {
                return;
            }
            //查询数据，并校验
            Record[] recordCopy = node.recordEventExecute().records();
            TapTable targetTableModel = super.getTargetTable(node.connectorNode());
            List<Map<String, Object>> result = super.queryRecords(node, targetTableModel, recordCopy);
            final int filterCount = result.size();
            if (filterCount != recordCount) {
                TapAssert.error(testCase, langUtil.formatLang("sequentialTest.more.fail", filterCount, recordCount));
            } else {
                Map<String, Object> resultMap = result.get(0);
                StringBuilder builder = new StringBuilder();
                //node.connectorNode().getCodecsFilterManager().transformToTapValueMap(resultMap, targetTableModel.getNameFieldMap());
                //TapCodecsFilterManager.create(TapCodecsRegistry.create()).transformFromTapValueMap(resultMap);

                boolean equals = super.mapEquals(transform(node, targetTableModel, recordCopy[0]), resultMap, builder, targetTableModel.getNameFieldMap());
                TapAssert.asserts(() -> {
                    Assertions.assertTrue(equals, langUtil.formatLang("sequentialTest.more.notEquals", filterCount, builder.toString()));
                }).acceptAsWarn(testCase, langUtil.formatLang("sequentialTest.more.succeed", filterCount, builder.toString()));
            }
        }, (node, testCase) -> {
            //删除表
            if (hasCreatedTable.get()) {
                RecordEventExecute execute = node.recordEventExecute();
                execute.dropTable();
            }
        });
    }

    private boolean modifyRecord(RecordEventExecute execute, int times) {
        Record.modifyRecordWithTapTable(super.targetTable, this.records, 5, false);
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

    private boolean insertAndModify(TestNode node, AtomicBoolean hasCreatedTable, boolean createNewRecord, int mTimes) {
        RecordEventExecute execute = node.recordEventExecute();
        Method testCase = execute.testCase();
        if (createNewRecord) {
            hasCreatedTable.set(super.createTable(node));
            if (!hasCreatedTable.get()) {
                //@TODO 建表失败
                return false;
            }
            execute.builderRecord(this.records);
        }
        WriteListResult<TapRecordEvent> insert;
        try {
            //插入数据
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
        for (int i = 0; i < mTimes; i++) {
            if (!this.modifyRecord(execute, i + 1)) {
                return false;
            }
        }
        return true;
    }

    private boolean insertAndModify(TestNode node, AtomicBoolean hasCreatedTable) {
        return this.insertAndModify(node, hasCreatedTable, true, modifyCount);
    }

    private boolean deleteRecord(TestNode node, int modifyCount) {
        RecordEventExecute execute = node.recordEventExecute();
        Method testCase = execute.testCase();
        WriteListResult<TapRecordEvent> delete = null;
        try {
            delete = node.recordEventExecute().delete();
        } catch (Throwable e) {
            TapAssert.error(testCase, langUtil.formatLang("sequentialTest.deleteRecord.throw", recordCount, modifyCount, e.getMessage()));
            return Boolean.FALSE;
        }
        WriteListResult<TapRecordEvent> finalDelete = delete;
        TapAssert.asserts(() ->
                Assertions.assertTrue(
                        null != finalDelete && finalDelete.getRemovedCount() == recordCount,
                        langUtil.formatLang("sequentialTest.deleteRecord.fail",
                                recordCount,
                                modifyCount,
                                null == finalDelete ? 0 : finalDelete.getInsertedCount(),
                                null == finalDelete ? 0 : finalDelete.getModifiedCount(),
                                null == finalDelete ? 0 : finalDelete.getRemovedCount())
                )
        ).acceptAsError(node.recordEventExecute().testCase(), langUtil.formatLang("sequentialTest.deleteRecord.succeed",
                recordCount,
                modifyCount,
                null == delete ? 0 : delete.getInsertedCount(),
                null == delete ? 0 : delete.getModifiedCount(),
                null == delete ? 0 : delete.getRemovedCount()));
        return null != finalDelete && finalDelete.getRemovedCount() == recordCount;
    }
}
