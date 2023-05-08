package io.tapdata.pdk.tdd.tests.v3;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.StreamReadFunction;
import io.tapdata.pdk.apis.functions.connector.source.TimestampToStreamOffsetFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.pdk.tdd.core.PDKTestBaseV2;
import io.tapdata.pdk.tdd.core.SupportFunction;
import io.tapdata.pdk.tdd.core.base.StreamStopException;
import io.tapdata.pdk.tdd.core.base.TapAssertException;
import io.tapdata.pdk.tdd.core.base.TddConfigKey;
import io.tapdata.pdk.tdd.core.base.TestNode;
import io.tapdata.pdk.tdd.tests.basic.RecordEventExecute;
import io.tapdata.pdk.tdd.tests.support.LangUtil;
import io.tapdata.pdk.tdd.tests.support.Record;
import io.tapdata.pdk.tdd.tests.support.TapAssert;
import io.tapdata.pdk.tdd.tests.support.TapGo;
import io.tapdata.pdk.tdd.tests.support.TapTestCase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.entity.simplify.TapSimplify.list;
import static io.tapdata.entity.simplify.TapSimplify.toJson;

/**
 * 增量验证（依赖StreamReadFunction和TimestampToStreamOffsetFunction）
 * 启动增量可能会比较慢， 超时时间可以设置为20分钟
 * 使用TimestampToStreamOffsetFunction获得当前时间的offset对象， 新建一张表进行增量测试
 * 测试失败按错误上报
 */
@DisplayName("checkStreamReadTest")
@TapGo(tag = "V3", sort = 10070, debug = false)
public class CheckStreamReadTest extends PDKTestBaseV2 {
    {
        if (PDKTestBaseV2.testRunning) {
            System.out.println(langUtil.formatLang("checkStreamRead.wait"));
        }
    }

    public static List<SupportFunction> testFunctions() {
        return list(
                support(WriteRecordFunction.class, LangUtil.format(inNeedFunFormat, "WriteRecordFunction")),
                support(StreamReadFunction.class, LangUtil.format(inNeedFunFormat, "StreamReadFunction")),
                support(TimestampToStreamOffsetFunction.class, LangUtil.format(inNeedFunFormat, "TimestampToStreamOffsetFunction"))
        );
    }

    private final static int recordCount = 3;
    private final static int updateCount = 1;
    private final static int deleteCount = 1;
    private final static int delayBound = 5;//s
    private final Object waitSingle = new Object();
    //private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    ScheduledThreadPoolExecutor task = new ScheduledThreadPoolExecutor(2);

    /**
     * 用例1，增量启动成功后写入数据
     * 开启增量启动成功之后
     * StreamReadConsumer#streamReadStarted方法需要被数据源调用;
     * 以下测试依赖WriteRecordFunction
     * <p>
     * 利用WriteRecordFunction写入3条数据， 能在5秒内通过接收StreamReadConsumer抛出的InsertRecord数据，
     * 来验证数据是按顺序接收到的；
     * <p>
     * 修改其中一条数据的多个字段， 能在5秒内通过接收StreamReadConsumer抛出来的UpdateRecord数据，
     * 来验证UpdateRecord数据的after是包含修改内容的（输出打印， after是全字段还是修改字段， 这个信息是大家会关注的）， before至少包含主键KV或者在after中能找到；
     * <p>
     * 删除其中一条数据， 能在5秒内通过接收StreamReadConsumer抛出来的DeleteRecord数据，
     * 来验证DeleteRecord数据的before是至少包含主键信息的
     */
    @DisplayName("checkStreamRead.batch")
    @TapTestCase(sort = 1)
    @Test
    public void check() throws NoSuchMethodException {
        System.out.println(langUtil.formatLang("checkStreamRead.stream.wait"));
        AtomicBoolean hasCreatedTable = new AtomicBoolean(false);
        AtomicReference<Integer> operatorType = new AtomicReference<>(-1);
        AtomicReference<StreamReadEntity> reference = new AtomicReference<>();
        AtomicBoolean stop = new AtomicBoolean(false);
        super.execTest((node, testCase) -> {
            Number tddConfig = node.recordEventExecute().findTddConfig(TddConfigKey.INCREMENTAL_DELAY_SEC.KeyName(), Number.class);
            final int incrementalDelaySec = Objects.isNull(tddConfig) ? (Integer) TddConfigKey.INCREMENTAL_DELAY_SEC.defaultValue() : tddConfig.intValue();
            hasCreatedTable.set(super.createTable(node));
            if (!hasCreatedTable.get()) {
                // 建表失败
                return;
            }

            ConnectorFunctions connectorFunctions = node.connectorNode().getConnectorFunctions();
            TimestampToStreamOffsetFunction timestampToStreamOffsetFunction = connectorFunctions.getTimestampToStreamOffsetFunction();
            Object streamOffset = null;
            if (timestampToStreamOffsetFunction == null) {
                streamOffset = System.currentTimeMillis();
            } else {
                try {
                    streamOffset = timestampToStreamOffsetFunction.timestampToStreamOffset(node.connectorNode().getConnectorContext(), null);
                } catch (Throwable throwable) {
                    streamOffset = System.currentTimeMillis();
                }
            }
            Record[] records = Record.testRecordWithTapTable(super.targetTable, recordCount);
            StreamReadConsumer consumerRead = StreamReadConsumer.create((events, offset) -> {
                if (Objects.nonNull(events) && !events.isEmpty()) {
                    synchronized (reference) {
                        reference.set(StreamReadEntity.create(events, offset, System.nanoTime()));
                    }
                    synchronized (operatorType) {
                        operatorType.set(-1);
                    }
                }
            }).stateListener((from, to) -> {
                //  开启增量启动成功之后，StreamReadConsumer#streamReadStarted方法需要被数据源调用;
                if (Objects.isNull(to) || !to.equals(StreamReadConsumer.STATE_STREAM_READ_STARTED)) {
                    //增量未手动开启
                    TapAssert.error(testCase, langUtil.formatLang("checkStreamRead.stream.notOpen"));
                    synchronized (stop) {
                        stop.set(true);
                    }
                } else {
                    //增量已开启
                    TapAssert.succeed(testCase, langUtil.formatLang("checkStreamRead.stream.opened"));
                    task.schedule(() -> {
                        try {
                            //写入三条数据
                            if (!insert(node, testCase, records, reference, operatorType)) {
                                //throw new StreamStopException("Stream need over.");
                                synchronized (stop) {
                                    stop.set(true);
                                }
                                return;
                            }
                            //修改一条数据
                            if (!update(node, testCase, records, reference, operatorType)) {
                                //throw new StreamStopException("Stream need over.");
                                synchronized (stop) {
                                    stop.set(true);
                                }
                                return;
                            }
                            //删除一条数据
                            if (!delete(node, testCase, records, reference, operatorType)) {
                                //throw new StreamStopException("Stream need over.");
                                synchronized (stop) {
                                    stop.set(true);
                                }
                                return;
                            }
                        } finally {
                            synchronized (stop) {
                                stop.set(true);
                            }
                        }
                        //throw new StreamStopException("Stream need over.");
                    }, incrementalDelaySec, TimeUnit.SECONDS);
                }
            });
            ConnectorFunctions functions = node.connectorNode().getConnectorFunctions();
            StreamReadFunction streamRead = functions.getStreamReadFunction();
            Object finalStreamOffset = streamOffset;
            task.schedule(() -> {
                try {
                    streamRead.streamRead(
                            node.connectorNode().getConnectorContext(),
                            list(super.targetTable.getId()),
                            finalStreamOffset,
                            200,
                            consumerRead);
                } catch (Throwable throwable) {
                    if (!(throwable instanceof TapAssertException) && !(throwable instanceof StreamStopException)) {
                        String msg = throwable.getMessage();
                        if (Objects.isNull(msg)) {
                            try {
                                msg = ((ExceptionInInitializerError) throwable).getException().getMessage();
                            } catch (Exception ignored) {

                            }
                        }
                        // 发生异常  退出
                        TapAssert.error(testCase, langUtil.formatLang("checkStreamRead.stream.throw", msg));
                    } else if (throwable instanceof TapAssertException) {
                        throw (TapAssertException) throwable;
                    } else {
                        throw (StreamStopException) throwable;
                    }
                } finally {
                    synchronized (stop) {
                        stop.set(true);
                    }
                }
            }, 0, TimeUnit.SECONDS);
            while (true) {
                synchronized (stop) {
                    if (stop.get()) {
                        task.shutdown();
                        break;
                    }
                    try {
                        stop.wait(1000);
                    } catch (Exception e) {

                    }
                }
            }
        }, (node, testCase) -> {
            if (hasCreatedTable.get()) {
                Optional.ofNullable(node.recordEventExecute()).ifPresent(RecordEventExecute::dropTable);
            }
            synchronized (operatorType) {
                operatorType.set(-2);
            }
            synchronized (waitSingle1) {
                waitSingle1.notifyAll();
            }
            waitSingle.notifyAll();
        });
    }

    private boolean insert(TestNode node, Method testCase, Record[] records, AtomicReference<StreamReadEntity> reference, AtomicReference<Integer> operatorType) {
        RecordEventExecute execute = node.recordEventExecute();
        execute.builderRecordCleanBefore(records);
        synchronized (operatorType) {
            operatorType.set(0);//插入标志
        }
        WriteListResult<TapRecordEvent> insert;
        try {
            insert = execute.insert();
        } catch (Throwable t) {
            //插入异常
            TapAssert.errorNotThrow(testCase, langUtil.formatLang("checkStreamRead.insert.throw",
                    recordCount,
                    t.getMessage()));
            return Boolean.TRUE;
        }
        if (null != insert && insert.getInsertedCount() == recordCount) {
            TapAssert.succeed(testCase, langUtil.formatLang("checkStreamRead.insert",
                    recordCount,
                    insert.getInsertedCount(),
                    insert.getModifiedCount(),
                    insert.getRemovedCount()));
        } else {
            TapAssert.errorNotThrow(testCase,
                    langUtil.formatLang("checkStreamRead.fail",
                            recordCount,
                            null == insert ? 0 : insert.getInsertedCount(),
                            null == insert ? 0 : insert.getModifiedCount(),
                            null == insert ? 0 : insert.getRemovedCount()));
            return Boolean.TRUE;
        }
        Long writeTime = System.nanoTime();
        while (true) {
            synchronized (operatorType) {
                if (operatorType.get() != 0) break;
            }
            try {
                synchronized (waitSingle1) {
                    waitSingle1.wait(1000);
                }
            } catch (InterruptedException ignored) {
            }
        }
        StreamReadEntity readEntity = null;
        synchronized (reference) {
            readEntity = reference.get();
        }
        List<TapEvent> event = readEntity.event();
        Long readTime = readEntity.readTime();
        // 验证延迟 5s内
        float delay = (readTime - writeTime) / 100_000_0000.00F;
        if (delay > delayBound) {
            TapAssert.errorNotThrow(testCase, langUtil.formatLang("checkStreamRead.insert.delay", recordCount, delay));
            return Boolean.TRUE;
        } else {
            TapAssert.succeed(testCase, langUtil.formatLang("checkStreamRead.insert.timely", recordCount, delay <= 0 ? (readTime - writeTime) * 0.000001F : delay));
        }
        Record[] recordCopy = execute.records();
        if (event.size() != recordCount) {
            TapAssert.errorNotThrow(testCase, langUtil.formatLang("checkStreamRead.insert.countError", recordCount, recordCount, event.size()));
            return Boolean.TRUE;
        }
        for (int index = 0; index < event.size(); index++) {
            TapEvent tapEvent = event.get(index);
            if (!(tapEvent instanceof TapInsertRecordEvent)) {
                //返回的结果不是插入事件
                TapAssert.errorNotThrow(testCase, langUtil.formatLang("checkStreamRead.insert.typeError", recordCount, index + 1, recordCount));
                return Boolean.TRUE;
            }
            TapInsertRecordEvent eventEnt = (TapInsertRecordEvent) tapEvent;
            Map<String, Object> after = eventEnt.getAfter();
            // 检查数据顺序
            Collection<String> primaryKeys = super.targetTable.primaryKeys(true);
            boolean equals = true;
            for (String key : primaryKeys) {
                Object keyValue1 = recordCopy[index].get(key);
                Object keyValue2 = after.get(key);
                if (!(equals = Objects.equals(keyValue1, keyValue2))) {
                    break;
                }
            }
            if (!equals) {
                //顺序不对
                TapAssert.errorNotThrow(testCase, langUtil.formatLang("checkStreamRead.insert.orderError", recordCount, index + 1));
                return Boolean.TRUE;
            }
        }
        return Boolean.TRUE;
    }

    private boolean delete(TestNode node, Method testCase, Record[] records, AtomicReference<StreamReadEntity> reference, AtomicReference<Integer> operatorType) {
        RecordEventExecute execute = node.recordEventExecute();
        Record[] r = new Record[1];
        for (int i = 0; i < deleteCount; i++) {
            r[i] = records[i];
        }
        execute.builderRecordCleanBefore(r);
        synchronized (operatorType) {
            operatorType.set(2);//删除标志
        }
        WriteListResult<TapRecordEvent> delete;
        try {
            delete = execute.delete();
        } catch (Throwable t) {
            // 删除异常
            TapAssert.errorNotThrow(testCase, langUtil.formatLang("checkStreamRead.delete.throw",
                    recordCount,
                    updateCount,
                    deleteCount,
                    t.getMessage()));
            return Boolean.FALSE;
        }
        if (null != delete && delete.getRemovedCount() == deleteCount) {
            TapAssert.succeed(testCase, langUtil.formatLang("checkStreamRead.delete",
                    recordCount,
                    updateCount,
                    deleteCount,
                    delete.getInsertedCount(),
                    delete.getModifiedCount(),
                    delete.getRemovedCount()));
        } else {
            TapAssert.errorNotThrow(testCase,
                    langUtil.formatLang("checkStreamRead.delete.fail",
                            recordCount,
                            updateCount,
                            deleteCount,
                            null == delete ? 0 : delete.getInsertedCount(),
                            null == delete ? 0 : delete.getModifiedCount(),
                            null == delete ? 0 : delete.getRemovedCount()));
            return Boolean.FALSE;
        }
        Long writeTime = System.nanoTime();
        while (true) {
            synchronized (operatorType) {
                if (operatorType.get() != 2) {
                    break;
                }
            }
            try {
                synchronized (waitSingle1) {
                    waitSingle1.wait(1000);
                }
            } catch (InterruptedException ignored) {
            }
        }
        StreamReadEntity readEntity = null;
        synchronized (reference) {
            readEntity = reference.get();
        }
        List<TapEvent> event = readEntity.event();
        Long readTime = readEntity.readTime();
        // 验证延迟 5s内
        float delay = (readTime - writeTime) / 1_000_000_000.00F;
        if (delay > delayBound) {
            TapAssert.errorNotThrow(testCase, langUtil.formatLang("checkStreamRead.delete.delay", recordCount, updateCount, deleteCount, delay));
            return Boolean.FALSE;
        } else {
            TapAssert.succeed(testCase, langUtil.formatLang("checkStreamRead.delete.timely", recordCount, updateCount, deleteCount, delay <= 0 ? (readTime - writeTime) * 0.000001F : delay));
        }
        TapTable targetTableModel = getTargetTable(node.connectorNode());
        for (int index = 0; index < event.size(); index++) {
            TapEvent tapEvent = event.get(index);
            if (!(tapEvent instanceof TapDeleteRecordEvent)) {
                //返回的结果不是删除事件
                TapAssert.errorNotThrow(testCase, langUtil.formatLang("checkStreamRead.delete.typeError", recordCount, updateCount, deleteCount, index + 1, deleteCount));
                return Boolean.FALSE;
            }
            TapDeleteRecordEvent eventEnt = (TapDeleteRecordEvent) tapEvent;
            Map<String, Object> before = transform(node, targetTableModel, eventEnt.getBefore());
            if (Objects.isNull(before) || before.isEmpty()) {
                TapAssert.errorNotThrow(testCase, langUtil.formatLang("checkStreamRead.delete.noKeys", recordCount, updateCount, deleteCount, index + 1, deleteCount));
                return Boolean.FALSE;
            }
            // 检查数据顺序
            Collection<String> primaryKeys = Optional.ofNullable(super.targetTable.primaryKeys(true)).orElse(super.targetTable.getNameFieldMap().keySet());
            boolean equals = true;
            Record[] recordCopy = execute.records();
            for (String key : primaryKeys) {
                Object keyValue1 = recordCopy[index].get(key);
                Object keyValue2 = before.get(key);
                if (!(equals = Objects.nonNull(keyValue2))) {
                    //主键不能为空
                    TapAssert.warn(testCase, langUtil.formatLang("checkStreamRead.delete.keyError", recordCount, updateCount, deleteCount, index + 1, toJson(primaryKeys), toJson(before)));
                    break;
                }
                if (!(equals = Objects.equals(keyValue1, keyValue2))) {
                    //变更前后主键需要相同
                    TapAssert.warn(testCase, langUtil.formatLang("checkStreamRead.delete.orderError", recordCount, updateCount, deleteCount, index + 1, toJson(primaryKeys), toJson(before), key, toJson(keyValue1)));
                    break;
                }
            }
            if (!equals) {
                //顺序不对
                return Boolean.FALSE;
            }
        }
        return Boolean.TRUE;
    }

    private boolean update(TestNode node, Method testCase, Record[] records, AtomicReference<StreamReadEntity> reference, AtomicReference<Integer> operatorType) {
        RecordEventExecute execute = node.recordEventExecute();
        Record[] r = new Record[updateCount];
        for (int i = 0; i < updateCount; i++) {
            r[i] = records[i];
        }
        Record.modifyRecordWithTapTable(super.targetTable, r, 5, false);
        execute.builderRecordCleanBefore(r);
        synchronized (operatorType) {
            operatorType.set(1);//修改标志
        }
        WriteListResult<TapRecordEvent> update;
        try {
            update = execute.update();
        } catch (Throwable t) {
            //修改异常
            TapAssert.errorNotThrow(testCase, langUtil.formatLang("checkStreamRead.update.throw",
                    recordCount,
                    updateCount,
                    t.getMessage()));
            return Boolean.TRUE;
        }
        if (null != update && update.getModifiedCount() == updateCount) {
            TapAssert.succeed(testCase, langUtil.formatLang("checkStreamRead.update",
                    recordCount,
                    updateCount,
                    update.getInsertedCount(),
                    update.getModifiedCount(),
                    update.getRemovedCount()));
        } else {
            TapAssert.errorNotThrow(testCase,
                    langUtil.formatLang("checkStreamRead.update.fail",
                            recordCount,
                            updateCount,
                            null == update ? 0 : update.getInsertedCount(),
                            null == update ? 0 : update.getModifiedCount(),
                            null == update ? 0 : update.getRemovedCount()));
            return Boolean.TRUE;
        }
        Long writeTime = System.nanoTime();
        while (true) {
            synchronized (operatorType) {
                if (operatorType.get() != 1) break;
            }
            try {
                synchronized (waitSingle1) {
                    waitSingle1.wait(1000);
                }
            } catch (InterruptedException ignored) {
            }
        }
        StreamReadEntity readEntity = null;
        synchronized (reference) {
            readEntity = reference.get();
        }
        List<TapEvent> event = readEntity.event();
        Long readTime = readEntity.readTime();
        // 验证延迟 5s内
        float delay = (readTime - writeTime) / 1_000_000_000.00F;
        if (delay > delayBound) {
            TapAssert.errorNotThrow(testCase, langUtil.formatLang("checkStreamRead.update.delay", recordCount, updateCount, delay));
            return Boolean.TRUE;
        } else {
            TapAssert.succeed(testCase, langUtil.formatLang("checkStreamRead.update.timely", recordCount, updateCount, delay <= 0 ? (readTime - writeTime) * 0.000001F : delay));
        }

        TapTable targetTableModel = super.getTargetTable(node.connectorNode());
        // 验证after是否包含主键内容
        for (int index = 0; index < event.size(); index++) {
            TapEvent tapEvent = event.get(index);
            if (!(tapEvent instanceof TapUpdateRecordEvent)) {
                //返回的结果不是修改事件
                TapAssert.errorNotThrow(testCase, langUtil.formatLang("checkStreamRead.update.typeError",
                        recordCount,
                        updateCount,
                        index + 1,
                        updateCount));
                return Boolean.FALSE;
            }
            TapUpdateRecordEvent eventEnt = (TapUpdateRecordEvent) tapEvent;
            Map<String, Object> before = transform(node, targetTableModel, eventEnt.getBefore());
            Map<String, Object> after = transform(node, targetTableModel, eventEnt.getAfter());
            if (Objects.isNull(before) || before.isEmpty()) {
                //修改事件的before中无键值信息，至少应该包含主键，告警
                TapAssert.warn(testCase, langUtil.formatLang("checkStreamRead.update.noKeys",
                        recordCount,
                        updateCount,
                        index + 1));
                //return Boolean.FALSE;
            }

            // 通过主键检查数据顺序
            Collection<String> primaryKeys = Optional.ofNullable(super.targetTable.primaryKeys(true)).orElse(super.targetTable.getNameFieldMap().keySet());
            boolean equals = true;
            Record[] recordCopy = execute.records();
            for (String key : primaryKeys) {
                Object keyValue1 = recordCopy[index].get(key);
                Object keyValue2 = after.get(key);
                if (!(equals = !Objects.isNull(keyValue2))) {
                    //主键不能为空
                    TapAssert.errorNotThrow(testCase, langUtil.formatLang("checkStreamRead.update.keyError", recordCount, updateCount, index + 1));
                    break;
                }
                if (!(equals = Objects.equals(keyValue1, keyValue2))) {
                    //变更前后主键需要相同
                    TapAssert.errorNotThrow(testCase, langUtil.formatLang("checkStreamRead.update.orderError", recordCount, updateCount, index + 1));
                    break;
                }
            }
            if (!equals) {
                //顺序不对或主键不对，前面已经打印了错误信息，这里直接返回
                return Boolean.TRUE;
            }

            //检查after数据是否包含全部，警告
            if (after.size() < r[0].size()) {
                TapAssert.warn(testCase, langUtil.formatLang("checkStreamRead.update.incomplete",
                        recordCount,
                        updateCount,
                        index + 1,
                        "\n" + LangUtil.SPILT_GRADE_4 + LangUtil.SPILT_GRADE_1 + toJson(after) + "\n" + LangUtil.SPILT_GRADE_4,
                        "\n" + LangUtil.SPILT_GRADE_4 + LangUtil.SPILT_GRADE_1 + toJson(r[0])));
            } else {
                TapAssert.succeed(testCase, langUtil.formatLang("checkStreamRead.update.complete",
                        recordCount,
                        updateCount,
                        index + 1,
                        "\n" + LangUtil.SPILT_GRADE_4 + LangUtil.SPILT_GRADE_1 + toJson(after) + "\n" + LangUtil.SPILT_GRADE_4,
                        "\n" + LangUtil.SPILT_GRADE_4 + LangUtil.SPILT_GRADE_1 + toJson(r[0])));
            }

            StringBuilder builder = new StringBuilder();

            //TapTable targetTableModel = super.getTargetTable(node.connectorNode());
            equals = super.mapEquals(transform(node, targetTableModel, r[0]), after, builder, targetTableModel.getNameFieldMap());
            if (!equals) {
                //修改前后数据进行比对
                TapAssert.warn(testCase, langUtil.formatLang("checkStreamRead.update.notEquals", recordCount, updateCount, index + 1, builder.toString()));
            } else {
                TapAssert.succeed(testCase, langUtil.formatLang("checkStreamRead.update.equals", recordCount, updateCount, index + 1));
            }
        }
        return Boolean.TRUE;
    }

    final Object waitSingle1 = new Object();

    public synchronized void waits(WaitCheck wait) {
        while (wait.to()) {
            try {
                waitSingle1.wait(1000);
            } catch (InterruptedException ignored) {
            }
        }
    }
}

interface WaitCheck {
    public boolean to();
}

class StreamReadEntity {
    List<TapEvent> event;
    Object offset;
    Long readTime;

    public static StreamReadEntity create(List<TapEvent> event, Object offset, Long readTime) {
        StreamReadEntity entity = new StreamReadEntity();
        entity.event = event;
        entity.offset = offset;
        entity.readTime = readTime;
        return entity;
    }

    public List<TapEvent> event() {
        return this.event;
    }

    public Object offset() {
        return this.offset;
    }

    public Long readTime() {
        return this.readTime;
    }
}