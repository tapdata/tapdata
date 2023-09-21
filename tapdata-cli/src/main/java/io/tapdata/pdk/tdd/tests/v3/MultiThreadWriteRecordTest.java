package io.tapdata.pdk.tdd.tests.v3;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connector.target.QueryByAdvanceFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryByFilterFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.tdd.core.PDKTestBaseV2;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.core.base.TapAssertException;
import io.tapdata.pdk.tdd.core.base.TestNode;
import io.tapdata.pdk.tdd.tests.basic.RecordEventExecute;
import io.tapdata.pdk.tdd.tests.support.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * WriteRecord的多线程支持（依赖WriteRecordFunction和QueryByAdvanceFilter/QueryByFilter）
 * Connector只用init/destroy一次， 然后用多线程调用writeRecord方法
 * 测试失败按警告上报
 */
@DisplayName("multiWriteTest")
@TapGo(tag = "V3", sort = 10040, debug = false)
public class MultiThreadWriteRecordTest extends PDKTestBaseV2 {
    {
        if (PDKTestBaseV2.testRunning) {
            System.out.println(langUtil.formatLang("multiWrite.wait"));
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
    private static final int modifyTimes = 4;
    private static final int threadCount = 4;

    /**
     * 用例1， 采用4个线程写入修改测试
     * 采用4个线程写入， 每个线程对各自不同主键的内容进行新增，
     * 修改1， 修改2， 修改3， 修改4事件， 最后的时候， 查询这条数据应该是修改4的内容
     */
    @DisplayName("multiWrite.modify")
    @TapTestCase(sort = 1)
    @Test
    public void multiModify() throws NoSuchMethodException {
        System.out.println(langUtil.formatLang("multiWrite.modify.wait"));
        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        AtomicInteger times = new AtomicInteger(threadCount);
        final Object insertLock = new Object();
        AtomicBoolean hasCreatedTable = new AtomicBoolean(false);
        super.execTest((node, testCase) -> {
            hasCreatedTable.set(super.createTable(node));
            if (!hasCreatedTable.get()) {
                // 建表失败
                return;
            }

            RecordEventExecute[] execute = new RecordEventExecute[threadCount];
            for (int index = 0; index < threadCount; index++) {
                final int finalIndex = index;
                execute[finalIndex] = RecordEventExecute.create(node.connectorNode(), this);
                execute[finalIndex].testCase(testCase);
                Record[] records = Record.testRecordWithTapTable(super.targetTable, 1);
                service.execute(() -> {
//                new Thread(()->{
                    try {
                        this.insertAndModify(execute[finalIndex], node, records);
                    } catch (Throwable e) {
                        if (!(e instanceof TapAssertException) || times.get() <= 0) {
                            throw e;
                        }
                    } finally {
                        synchronized (insertLock) {
                            times.decrementAndGet();
                        }
                    }
                });//.start();
            }

            while (times.get() > 0) {
                try {
                    synchronized (insertLock) {
                        insertLock.wait(1000);
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }, (node, testCase) -> {
            //删除表
            if (hasCreatedTable.get()) {
                RecordEventExecute execute = node.recordEventExecute();
                execute.dropTable();
            }
            service.shutdown();
            insertLock.notifyAll();
        });
    }

    /**
     * 用例2， 采用4个线程删除测试
     * 采用4个线程写入， 每个线程对各自不同主键的内容进行新增，
     * 修改1， 修改2， 修改3， 修改4， 删除事件，
     * 最后的时候， 查询这条数据应该是为空
     */
    @DisplayName("multiWrite.delete")
    @TapTestCase(sort = 2)
    @Test
    public void multiDelete() throws NoSuchMethodException {
        System.out.println(langUtil.formatLang("multiWrite.delete.wait"));
        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        AtomicInteger times = new AtomicInteger(threadCount);
        final Object insertLock = new Object();
        AtomicBoolean hasCreatedTable = new AtomicBoolean(false);
        super.execTest((node, testCase) -> {
            if (!hasCreatedTable.get()) {
                if (!hasCreatedTable.get()) {
                    hasCreatedTable.set(super.createTable(node));
                }
            }
            if (!hasCreatedTable.get()) {
                // 建表失败
                return;
            }

            RecordEventExecute[] execute = new RecordEventExecute[threadCount];
            for (int index = 0; index < threadCount; index++) {
                int finalIndex = index;
                execute[finalIndex] = RecordEventExecute.create(node.connectorNode(), this);
                execute[finalIndex].testCase(testCase);
                Record[] records = Record.testRecordWithTapTable(super.targetTable, 1);
                service.execute(() -> {
                    //new Thread(()->{
                    try {
                        this.insertAndDelete(execute[finalIndex], node, records);
                    } catch (Throwable e) {
                        if (!(e instanceof TapAssertException) || times.get() <= 0) {
                            throw e;
                        }
                    } finally {
                        synchronized (insertLock) {
                            times.decrementAndGet();
                        }
                    }
                });//.start();
            }

            while (times.get() > 0) {
                try {
                    synchronized (insertLock) {
                        insertLock.wait(1000);
                    }
                } catch (InterruptedException e) {
                    //continue;
                }
            }
        }, (node, testCase) -> {
            //删除表
            if (hasCreatedTable.get()) {
                RecordEventExecute execute = node.recordEventExecute();
                execute.dropTable();
            }
            service.shutdown();
            insertLock.notifyAll();
        });
    }

    private void insertAndModify(RecordEventExecute execute, TestNode node, Record[] records) {
        if (!this.insertAndModify(execute, records, modifyTimes)) {
            return;
        }
        //查询数据，并校验
        Record[] recordCopy = execute.records();
        TapTable targetTableModel = super.getTargetTable(node.connectorNode());
        List<Map<String, Object>> result = super.queryRecords(node, targetTableModel, recordCopy);
        final int filterCount = result.size();
        final String tName = Thread.currentThread().getName();
        if (filterCount != recordCount) {
            TapAssert.error(execute.testCase(), langUtil.formatLang("multiWrite.modify.query.fail",
                    tName,
                    recordCount,
                    modifyTimes,
                    filterCount,
                    recordCount));
        } else {
            Map<String, Object> resultMap = result.get(0);
            StringBuilder builder = new StringBuilder();
            //node.connectorNode().getCodecsFilterManager().transformToTapValueMap(resultMap, targetTableModel.getNameFieldMap());
            //TapCodecsFilterManager.create(TapCodecsRegistry.create()).transformFromTapValueMap(resultMap);
            boolean equals = super.mapEquals(transform(node, targetTableModel, recordCopy[0]), resultMap, builder, targetTableModel.getNameFieldMap());
            TapAssert.asserts(() -> {
                Assertions.assertTrue(equals, langUtil.formatLang("multiWrite.modify.query.notEquals",
                        tName,
                        recordCount,
                        modifyTimes,
                        filterCount,
                        builder.toString()));
            }).acceptAsWarn(execute.testCase(), langUtil.formatLang("multiWrite.modify.query.succeed",
                    tName,
                    recordCount,
                    modifyTimes,
                    filterCount));
        }
    }

    private void insertAndDelete(RecordEventExecute execute, TestNode node, Record[] records) {
        if (!this.insertAndModify(execute, records, modifyTimes)) {
            return;
        }
        if (!this.deleteRecord(execute)) {
            return;
        }
        final String tName = Thread.currentThread().getName();
        //查询数据，并校验
        Record[] recordCopy = execute.records();
        TapTable targetTableModel = super.getTargetTable(node.connectorNode());
        List<Map<String, Object>> result = super.queryRecords(node, targetTableModel, recordCopy);
        if (result.isEmpty()){
            TapAssert.succeed(execute.testCase(),langUtil.formatLang("multiWrite.delete.succeed",
                    tName,
                    recordCount,
                    modifyTimes
            ));
        }else {
            TapAssert.error(execute.testCase(),langUtil.formatLang(
                    "multiWrite.delete.fail",
                    tName,
                    recordCount,
                    modifyTimes,
                    result.size()));
        }
    }

    private boolean insertAndModify(RecordEventExecute execute, Record[] records, int mTimes) {
        Method testCase = execute.testCase();
        WriteListResult<TapRecordEvent> insert;
        final String tName = Thread.currentThread().getName();
        execute.builderRecordCleanBefore(records);
        try {
            //插入数据
            //synchronized (this) {
            insert = execute.insert();
            //}
        } catch (Throwable e) {
            TapAssert.error(testCase, langUtil.formatLang("multiWrite.insertRecord.throw", tName, e.getMessage()));
            return false;
        }
        TapAssert.asserts(() ->
                Assertions.assertTrue(
                        null != insert && insert.getInsertedCount() == recordCount,
                        langUtil.formatLang("multiWrite.insertRecord.fail",
                                tName,
                                recordCount,
                                null == insert ? 0 : insert.getInsertedCount(),
                                null == insert ? 0 : insert.getModifiedCount(),
                                null == insert ? 0 : insert.getRemovedCount())
                )
        ).acceptAsError(testCase, langUtil.formatLang("multiWrite.insertRecord",
                tName,
                recordCount,
                null == insert ? 0 : insert.getInsertedCount(),
                null == insert ? 0 : insert.getModifiedCount(),
                null == insert ? 0 : insert.getRemovedCount()));
        //修改数据
        for (int i = 0; i < mTimes; i++) {
            if (!this.modifyRecord(execute, records, i + 1)) {
                return false;
            }
        }
        return true;
    }

    private boolean modifyRecord(RecordEventExecute execute, Record[] records, int times) {
        Record.modifyRecordWithTapTable(super.targetTable, records, modifyTimes, false);
        execute.builderRecordCleanBefore(records);
        WriteListResult<TapRecordEvent> update;
        final String tName = Thread.currentThread().getName();
        try {
            //synchronized (this){
            update = execute.update();
            //}
        } catch (Throwable throwable) {
            TapAssert.error(execute.testCase(), langUtil.formatLang("multiWrite.modifyRecord.throw",
                    tName,
                    recordCount,
                    times,
                    throwable.getMessage()));
            return Boolean.FALSE;
        }
        TapAssert.asserts(() ->
                Assertions.assertTrue(
                        null != update && update.getModifiedCount() == recordCount,
                        langUtil.formatLang("multiWrite.modifyRecord.fail",
                                tName,
                                recordCount,
                                times,
                                null == update ? 0 : update.getInsertedCount(),
                                null == update ? 0 : update.getModifiedCount(),
                                null == update ? 0 : update.getRemovedCount())
                )
        ).acceptAsError(execute.testCase(), langUtil.formatLang("multiWrite.modifyRecord",
                tName,
                recordCount,
                times,
                null == update ? 0 : update.getInsertedCount(),
                null == update ? 0 : update.getModifiedCount(),
                null == update ? 0 : update.getRemovedCount()));
        return null != update && update.getModifiedCount() == recordCount;
    }

    private boolean deleteRecord(RecordEventExecute execute) {
        Method testCase = execute.testCase();
        WriteListResult<TapRecordEvent> delete = null;
        final String tName = Thread.currentThread().getName();
        try {
            //synchronized (this) {
            delete = execute.delete();
            //}
        } catch (Throwable e) {
            TapAssert.error(testCase, langUtil.formatLang("multiWrite.deleteRecord.throw",
                    tName,
                    recordCount,
                    modifyTimes,
                    e.getMessage()));
            return Boolean.FALSE;
        }
        WriteListResult<TapRecordEvent> finalDelete = delete;
        TapAssert.asserts(() ->
                Assertions.assertTrue(
                        null != finalDelete && finalDelete.getRemovedCount() == recordCount,
                        langUtil.formatLang("multiWrite.deleteRecord.fail",
                                tName,
                                recordCount,
                                modifyTimes,
                                null == finalDelete ? 0 : finalDelete.getInsertedCount(),
                                null == finalDelete ? 0 : finalDelete.getModifiedCount(),
                                null == finalDelete ? 0 : finalDelete.getRemovedCount())
                )
        ).acceptAsError(execute.testCase(), langUtil.formatLang("multiWrite.deleteRecord.succeed",
                tName,
                recordCount,
                modifyTimes,
                null == delete ? 0 : delete.getInsertedCount(),
                null == delete ? 0 : delete.getModifiedCount(),
                null == delete ? 0 : delete.getRemovedCount()));
        return null != finalDelete && finalDelete.getRemovedCount() == recordCount;
    }
}
