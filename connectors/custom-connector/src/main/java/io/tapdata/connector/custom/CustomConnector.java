package io.tapdata.connector.custom;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.custom.config.CustomConfig;
import io.tapdata.connector.custom.core.Core;
import io.tapdata.connector.custom.core.ScriptCore;
import io.tapdata.connector.custom.util.ScriptUtil;
import io.tapdata.constant.ConnectionTypeEnum;
import io.tapdata.constant.SyncTypeEnum;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@TapConnectorClass("spec_custom.json")
public class CustomConnector extends ConnectorBase {

    private static final ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class); //script factory
    private CustomConfig customConfig;
    private ScriptEngine initScriptEngine;
    private ConcurrentHashMap<String, ScriptEngine> writeEnginePool;

    private void initConnection(TapConnectionContext connectorContext) throws ScriptException {
        customConfig = new CustomConfig().load(connectorContext.getConnectionConfig());
        writeEnginePool = new ConcurrentHashMap<>(16);
        assert scriptFactory != null;
        initScriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName(customConfig.getJsEngineName()));
        initScriptEngine.eval(ScriptUtil.appendBeforeFunctionScript(customConfig.getCustomBeforeScript()) + "\n"
                + ScriptUtil.appendAfterFunctionScript(customConfig.getCustomAfterScript()));
//        initScriptEngine.put("log", new CustomLog());
    }

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        initConnection(connectionContext);
        if (customConfig.getCustomBeforeOpr()) {
            ScriptUtil.executeScript(initScriptEngine, ScriptUtil.BEFORE_FUNCTION_NAME);
        }
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        try {
            if (initScriptEngine instanceof Closeable) {
                ((Closeable) initScriptEngine).close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void beforeStop() {
        if (EmptyKit.isNotNull(customConfig) && customConfig.getCustomAfterOpr()) {
            ScriptUtil.executeScript(initScriptEngine, ScriptUtil.AFTER_FUNCTION_NAME);
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        codecRegistry.registerFromTapValue(TapRawValue.class, "String", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return tapRawValue.getValue().toString();
            return "null";
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss"));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS"));
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd"));

        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        CustomSchema customSchema = new CustomSchema(customConfig);
        TapTable tapTable = customSchema.loadSchema();
        if (EmptyKit.isNotNull(tapTable)) {
            consumer.accept(Collections.singletonList(tapTable));
        }
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        initConnection(connectionContext);
        CustomTest customTest = new CustomTest(customConfig);
        TestItem testScript = customTest.testScript();
        consumer.accept(testScript);
        if (!ConnectionTypeEnum.TARGET.getType().equals(customConfig.get__connectionType()) && (testScript.getResult() != TestItem.RESULT_FAILED)) {
            consumer.accept(customTest.testBuildSchema());
        }
        return ConnectionOptions.create().connectionString("Custom Connection: " +
                (ConnectionTypeEnum.TARGET.getType().equals(customConfig.get__connectionType()) ? ConnectionTypeEnum.TARGET.getType() : "source[" + customConfig.getCollectionName() + "]"));
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) {
        return 1;
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws ScriptException {
        assert scriptFactory != null;
        String threadName = Thread.currentThread().getName();
        ScriptEngine scriptEngine;
        if (writeEnginePool.containsKey(threadName)) {
            scriptEngine = writeEnginePool.get(threadName);
        } else {
            scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName(customConfig.getJsEngineName()));
            scriptEngine.eval(ScriptUtil.appendTargetFunctionScript(customConfig.getTargetScript()));
//            scriptEngine.put("log", new CustomLog());
            writeEnginePool.put(threadName, scriptEngine);
        }
        List<Map<String, Object>> data = new ArrayList<>();
        WriteListResult<TapRecordEvent> result = new WriteListResult<>();
        AtomicLong insert = new AtomicLong(0);
        AtomicLong update = new AtomicLong(0);
        AtomicLong delete = new AtomicLong(0);
        for (TapRecordEvent event : tapRecordEvents) {
            Map<String, Object> temp = new HashMap<>();
            if (event instanceof TapInsertRecordEvent) {
                temp.put("data", ((TapInsertRecordEvent) event).getAfter());
                temp.put("op", Core.MESSAGE_OPERATION_INSERT);
                insert.incrementAndGet();
            } else if (event instanceof TapUpdateRecordEvent) {
                temp.put("data", ((TapUpdateRecordEvent) event).getAfter());
                temp.put("op", Core.MESSAGE_OPERATION_UPDATE);
                update.incrementAndGet();
            } else {
                temp.put("data", ((TapDeleteRecordEvent) event).getBefore());
                temp.put("op", Core.MESSAGE_OPERATION_DELETE);
                delete.incrementAndGet();
            }
            temp.put("from", tapTable.getId());
            data.add(temp);
        }
        ScriptUtil.executeScript(scriptEngine, ScriptUtil.TARGET_FUNCTION_NAME, data);
        writeListResultConsumer.accept(result.insertedCount(insert.get()).modifiedCount(update.get()).removedCount(delete.get()));
    }

    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) {
        return 0;
    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws ScriptException {
        ScriptCore scriptCore = new ScriptCore(tapTable.getId());
        assert scriptFactory != null;
        ScriptEngine scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName(customConfig.getJsEngineName()));
        scriptEngine.eval(ScriptUtil.appendSourceFunctionScript(customConfig.getHistoryScript(), true));
        scriptEngine.put("core", scriptCore);
//        scriptEngine.put("log", new CustomLog());
        AtomicReference<Throwable> scriptException = new AtomicReference<>();
        Runnable runnable = () -> {
            Invocable invocable = (Invocable) scriptEngine;
            try {
                invocable.invokeFunction(ScriptUtil.SOURCE_FUNCTION_NAME);
            } catch (Exception e) {
                scriptException.set(e);
            }
        };
        Thread t = new Thread(runnable);
        t.start();
        List<TapEvent> eventList = new ArrayList<>();
        while (isAlive() && t.isAlive()) {
            try {
                CustomEventMessage message = null;
                try {
                    message = scriptCore.getEventQueue().poll(1, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                }
                if (EmptyKit.isNotNull(message)) {
                    eventList.add(message.getTapEvent());
                    if (eventList.size() == eventBatchSize) {
                        eventsOffsetConsumer.accept(eventList, new HashMap<>());
                        eventList = new ArrayList<>();
                    }
                }
            } catch (Exception e) {
                break;
            }
        }
        if (EmptyKit.isNotNull(scriptException.get())) {
            throw new RuntimeException(scriptException.get());
        }
        if (isAlive() && EmptyKit.isNotEmpty(eventList)) {
            eventsOffsetConsumer.accept(eventList, new HashMap<>());
        }
        if (t.isAlive()) {
            t.stop();
        }
        if (customConfig.getSyncType().equals(SyncTypeEnum.INITIAL_SYNC.getType())) {
            beforeStop();
        }
    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        ScriptCore scriptCore = new ScriptCore(tableList.get(0));
        AtomicReference<Object> contextMap = new AtomicReference<>(offsetState);
        assert scriptFactory != null;
        ScriptEngine scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName(customConfig.getJsEngineName()));
        scriptEngine.eval(ScriptUtil.appendSourceFunctionScript(customConfig.getCdcScript(), false));
        scriptEngine.put("core", scriptCore);
//        scriptEngine.put("log", new CustomLog());
        AtomicReference<Throwable> scriptException = new AtomicReference<>();
        Runnable runnable = () -> {
            Invocable invocable = (Invocable) scriptEngine;
            try {
                while (isAlive()) {
                    invocable.invokeFunction(ScriptUtil.SOURCE_FUNCTION_NAME, contextMap.get());
                    Thread.sleep(1000);
                }
            } catch (InterruptedException ignored) {
            } catch (Exception e) {
                scriptException.set(e);
            }
        };
        Thread t = new Thread(runnable);
        t.start();
        consumer.streamReadStarted();
        List<TapEvent> eventList = new ArrayList<>();
        Object lastContextMap = null;
        long ts = System.currentTimeMillis();
        while (isAlive() && t.isAlive()) {
            CustomEventMessage message = null;
            try {
                message = scriptCore.getEventQueue().poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            }
            if (EmptyKit.isNotNull(message)) {
                eventList.add(message.getTapEvent());
                lastContextMap = message.getContextMap();
                if (eventList.size() == recordSize || (System.currentTimeMillis() - ts) >= 3000) {
                    consumer.accept(eventList, lastContextMap);
                    contextMap.set(lastContextMap);
                    eventList = new ArrayList<>();
                    ts = System.currentTimeMillis();
                }
            }
        }
        if (EmptyKit.isNotNull(scriptException.get())) {
            throw scriptException.get();
        }
        if (isAlive() && EmptyKit.isNotEmpty(eventList)) {
            consumer.accept(eventList, lastContextMap);
            contextMap.set(lastContextMap);
        }
        if (t.isAlive()) {
            t.stop();
        }
        consumer.streamReadEnded();
        beforeStop();
    }

    private Object timestampToStreamOffset(TapConnectorContext connectorContext, Long offsetStartTime) {
        return new HashMap<>();
    }

}
