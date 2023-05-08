package io.tapdata.pdk.tdd.tests.v4;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.pdk.tdd.core.PDKTestBaseV2;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.core.base.TestNode;
import io.tapdata.pdk.tdd.tests.basic.RecordEventExecute;
import io.tapdata.pdk.tdd.tests.support.LangUtil;
import io.tapdata.pdk.tdd.tests.support.Record;
import io.tapdata.pdk.tdd.tests.support.TapAssert;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.tapdata.entity.simplify.TapSimplify.list;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author GavinXiao
 * @description BatchReadPauseAndStreamReadTest create by Gavin
 * @create 2023/5/5 19:10
 * 模拟引擎的全量加增量过程， 读取全量过程中sleep停顿， 通过另外一个线程插入， 修改， 删除， 检查数据是否能一致
 **/
@DisplayName("batchPauseAndStream")
@TapGo(tag = "V4", sort = 20000, debug = false, ignore = false)
public class BatchReadPauseAndStreamReadTest extends PDKTestBaseV2 {
    {
        if (PDKTestBaseV2.testRunning) {
            System.out.println(langUtil.formatLang("batchPauseAndStream.wait"));
        }
    }

    public static List<SupportFunction> testFunctions() {
        return list(
            support(WriteRecordFunction.class, LangUtil.format(inNeedFunFormat, "WriteRecordFunction")),
            support(BatchReadFunction.class, LangUtil.format(inNeedFunFormat, "BatchReadFunction"))
        );
    }

    public static final int recordCountBase = 2;
    public static final int recordCount = 1;
    /**
     * 用例1，模拟引擎的全量加增量过程，
     * 读取全量过程中sleep停顿，
     * 通过另外一个线程插入，修改，删除，
     * 检查数据是否能一致
     *
     * 全量读取2条数据，
     * 在2条数据前插入一条数据，
     * 由于没有snapshot，
     * 应该能重复读出第二条数据，
     * 此时之后的逻辑是否能正常
     */
    @DisplayName("batchPauseAndStream.batch")
    @TapTestCase(sort = 1)
    @Test
    public void batch() throws NoSuchMethodException {
        System.out.println(langUtil.formatLang("batchPauseAndStream.batch.wait"));
        AtomicBoolean hasCreatedTable = new AtomicBoolean(false);
        ExecutorService service = Executors.newFixedThreadPool(1);
        super.execTest((node, testCase) -> {
            //创建表
            hasCreatedTable.set(createTable(node));
            if (!hasCreatedTable.get()) {
                //建表失败;
                return;
            }
            //插入2条数据
            Record[] recordsBase = Record.testRecordWithTapTable(targetTable, recordCountBase);
            insertRecords(node, recordsBase, false);

            ConnectorFunctions functions = node.connectorNode().getConnectorFunctions();
            BatchReadFunction readFunction = functions.getBatchReadFunction();
            final AtomicBoolean hasLocked = new AtomicBoolean(false);
            final List<TapEvent> eventList = new ArrayList<>();

            //开启线程
            service.execute(()->{
                for(;;){
                    synchronized (hasLocked){
                        if (hasLocked.get()) break;
                    }
                }
                //写入数据
                Record[] recordItem = Record.testRecordWithTapTable(targetTable, 1);
                insertRecords(node, recordItem, true);
                //修改数据
                modifyRecord(node.recordEventExecute(), recordItem);
                //删除数据
                deleteRecord(node.recordEventExecute());
                //notify()
                synchronized (hasLocked){
                    hasLocked.notifyAll();
                }
            });

            //batch read，并wait()
            try {
                readFunction.batchRead(
                    node.connectorNode().getConnectorContext(),
                    getTargetTable(node.connectorNode()),
                    null,
                    1,
                    (events,obj) -> {
                        if (null != events && !events.isEmpty()){
                            eventList.addAll(events);
                        }
                        if (!hasLocked.get()) {
                            synchronized (hasLocked) {
                                if (!hasLocked.get()){
                                    try {
                                        hasLocked.set(true);
                                        hasLocked.wait();
                                    } catch (InterruptedException ignored) {

                                    }
                                }
                            }
                        }
                    }
                );
            }catch (Throwable e){
                TapAssert.error(testCase, langUtil.formatLang("batchPauseAndStream.error.batch", e.getMessage()));
            }finally {
                synchronized (hasLocked){
                    hasLocked.set(true);
                }
            }

            //检查batch read结果是否一致
            //检查数量是否一致
            TapAssert.asserts(() ->
                Assertions.assertEquals(
                    eventList.size(),
                    recordCountBase,
                    langUtil.formatLang("batchPauseAndStream.count.fail", eventList.size(), recordCountBase))
            ).acceptAsError(
                testCase,
                langUtil.formatLang("batchPauseAndStream.count.succeed", eventList.size(), recordCountBase)
            );

            //检查内容是否一致
            Map<Long,Record> recordMap = new HashMap<>();
            final String primaryKey = "id";
            for (Record record : recordsBase) {
                recordMap.put((Long) record.get(primaryKey), record);
            }
            TapTable targetTableModel = super.getTargetTable(node.connectorNode());
            if (eventList.size() == recordCountBase) {
                for (int index = 0; index < eventList.size(); index++) {
                    TapEvent tapEvent = eventList.get(index);
                    final int no = index +1;
                    if (tapEvent instanceof TapInsertRecordEvent) {
                        TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) tapEvent;
                        Map<String, Object> after = insertRecordEvent.getAfter();
                        Long ID = (Long) after.get(primaryKey);
                        Record record = recordMap.get(ID);
                        Map<String, Object> recordMapItem = this.code(node.connectorNode(), InstanceFactory.instance(TapUtils.class).cloneMap(record));
                        StringBuilder builder = new StringBuilder();
                        TapAssert.asserts(() -> assertTrue(
                                mapEquals(transform(node, targetTableModel, recordMapItem), after, builder, targetTableModel.getNameFieldMap()),
                                langUtil.formatLang("batchPauseAndStream.exact.equals.failed", no, primaryKey, ID, builder.toString())
                        )).acceptAsWarn(testCase, langUtil.formatLang("batchPauseAndStream.exact.equals.succeed", no, primaryKey, ID, builder.toString()));
                    } else if (tapEvent instanceof TapUpdateRecordEvent){
                        TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) tapEvent;
                        Map<String, Object> after = updateRecordEvent.getAfter();
                        Long ID = (Long) after.get(primaryKey);
                        Record record = recordMap.get(ID);
                        Map<String, Object> recordMapItem = this.code(node.connectorNode(), InstanceFactory.instance(TapUtils.class).cloneMap(record));
                        StringBuilder builder = new StringBuilder();
                        TapAssert.asserts(() -> assertTrue(
                                mapEquals(transform(node, targetTableModel, recordMapItem), after, builder, targetTableModel.getNameFieldMap()),
                                langUtil.formatLang("batchPauseAndStream.exact.equals.failed",no, primaryKey, ID, builder.toString())
                        )).acceptAsWarn(testCase, langUtil.formatLang("batchPauseAndStream.exact.equals.succeed", no, primaryKey, ID, builder.toString()));
                    } else if (tapEvent instanceof TapDeleteRecordEvent){
                        TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) tapEvent;
                        Map<String, Object> before = deleteRecordEvent.getBefore();
                        Long ID = (Long) before.get(primaryKey);
                        Record record = recordMap.get(ID);
                        Map<String, Object> recordMapItem = this.code(node.connectorNode(), InstanceFactory.instance(TapUtils.class).cloneMap(record));
                        StringBuilder builder = new StringBuilder();
                        TapAssert.asserts(() -> assertTrue(
                                mapEquals(transform(node, targetTableModel, recordMapItem), before, builder, targetTableModel.getNameFieldMap()),
                                langUtil.formatLang("batchPauseAndStream.exact.equals.failed", no, primaryKey, ID, builder.toString())
                        )).acceptAsWarn(testCase, langUtil.formatLang("batchPauseAndStream.exact.equals.succeed", no, primaryKey, ID, builder.toString()));
                    }else {
                        TapAssert.error(testCase, langUtil.formatLang("batchPauseAndStream.error.event", no));
                    }
                }
            }
        }, (node, testCase) -> {
            //删除表
            if (hasCreatedTable.get()) {
                RecordEventExecute execute = node.recordEventExecute();
                execute.dropTable();
            }
            service.shutdown();
        });
    }

    private boolean insertRecords(TestNode node, Record[] records, boolean flat) {
        RecordEventExecute execute = node.recordEventExecute();
        Method testCase = execute.testCase();
        WriteListResult<TapRecordEvent> insert;
        //for (int index = 0; index < records.length; index++) {
            execute.builderRecordCleanBefore(records);
            //insertAfter[index] = execute.records()[0];
            //final int finalIndex = index + 1;
            //插入数据
            try {
                insert = execute.insert();
            } catch (Throwable e) {
                TapAssert.error(testCase, langUtil.formatLang(flat ? "batchPauseAndStream.insertRecord.flat.throw" : "batchPauseAndStream.insertRecord.throw",
                        records.length, e.getMessage()));
                return false;
            }
            WriteListResult<TapRecordEvent> finalInsert = insert;
            TapAssert.asserts(() ->
                Assertions.assertTrue(
                    null != finalInsert && finalInsert.getInsertedCount() == records.length,
                    langUtil.formatLang(flat ? "batchPauseAndStream.insertRecord.flat.fail" : "batchPauseAndStream.insertRecord.fail",
                            records.length,
                        null == finalInsert ? 0 : finalInsert.getInsertedCount(),
                        null == finalInsert ? 0 : finalInsert.getModifiedCount(),
                        null == finalInsert ? 0 : finalInsert.getRemovedCount())
                )
            ).acceptAsError(testCase, langUtil.formatLang(flat ? "batchPauseAndStream.insertRecord.flat" : "batchPauseAndStream.insertRecord",
                    records.length,
                null == insert ? 0 : insert.getInsertedCount(),
                null == insert ? 0 : insert.getModifiedCount(),
                null == insert ? 0 : insert.getRemovedCount()));
        //}
        return true;
    }

    final static int modifyTimes = 1;
    private boolean modifyRecord(RecordEventExecute execute, Record[] records) {
        Record.modifyRecordWithTapTable(super.targetTable, records, modifyTimes, false);
        execute.builderRecordCleanBefore(records);
        WriteListResult<TapRecordEvent> update;
        try {
            update = execute.update();
        } catch (Throwable throwable) {
            TapAssert.error(execute.testCase(), langUtil.formatLang("batchPauseAndStream.modifyRecord.throw",
                recordCount,
                throwable.getMessage()));
            return Boolean.FALSE;
        }
        TapAssert.asserts(() ->
            Assertions.assertTrue(
                null != update && update.getModifiedCount() == recordCount,
                langUtil.formatLang("batchPauseAndStream.modifyRecord.fail",
                    recordCount,
                    null == update ? 0 : update.getInsertedCount(),
                    null == update ? 0 : update.getModifiedCount(),
                    null == update ? 0 : update.getRemovedCount())
            )
        ).acceptAsError(execute.testCase(), langUtil.formatLang("batchPauseAndStream.modifyRecord",
            recordCount,
            null == update ? 0 : update.getInsertedCount(),
            null == update ? 0 : update.getModifiedCount(),
            null == update ? 0 : update.getRemovedCount()));
        return null != update && update.getModifiedCount() == recordCount;
    }

    private boolean deleteRecord(RecordEventExecute execute) {
        Method testCase = execute.testCase();
        WriteListResult<TapRecordEvent> delete = null;
        try {
            delete = execute.delete();
        } catch (Throwable e) {
            TapAssert.error(testCase, langUtil.formatLang("batchPauseAndStream.deleteRecord.throw",
                recordCount,
                e.getMessage()));
            return Boolean.FALSE;
        }
        WriteListResult<TapRecordEvent> finalDelete = delete;
        TapAssert.asserts(() ->
            Assertions.assertTrue(
                null != finalDelete && finalDelete.getRemovedCount() == recordCount,
                langUtil.formatLang("batchPauseAndStream.deleteRecord.fail",
                    recordCount,
                    null == finalDelete ? 0 : finalDelete.getInsertedCount(),
                    null == finalDelete ? 0 : finalDelete.getModifiedCount(),
                    null == finalDelete ? 0 : finalDelete.getRemovedCount())
            )
        ).acceptAsError(execute.testCase(), langUtil.formatLang("batchPauseAndStream.deleteRecord.succeed",
            recordCount,
            null == delete ? 0 : delete.getInsertedCount(),
            null == delete ? 0 : delete.getModifiedCount(),
            null == delete ? 0 : delete.getRemovedCount()));
        return null != finalDelete && finalDelete.getRemovedCount() == recordCount;
    }

    public Map<String, Object> code(ConnectorNode connectorNode, Map<String, Object> map){
        LinkedHashMap<String, TapField> nameFieldMap = getTargetTable(connectorNode).getNameFieldMap();
        TapCodecsFilterManager codecsFilterManager = new TapCodecsFilterManager(TapCodecsRegistry.create());
        codecsFilterManager.transformToTapValueMap(map,nameFieldMap);
        TapCodecsFilterManager targetCodecsFilterManager = connectorNode.getCodecsFilterManager();
        targetCodecsFilterManager.transformFromTapValueMap(map, nameFieldMap);
        return map;
    }
}