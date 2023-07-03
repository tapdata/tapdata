package io.tapdata.connector.custom;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.custom.config.CustomConfig;
import io.tapdata.connector.custom.core.Core;
import io.tapdata.connector.custom.core.ScriptCore;
import io.tapdata.connector.custom.util.ScriptUtil;
import io.tapdata.constant.ConnectionTypeEnum;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.script.ScriptOptions;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.FormatUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.commons.lang3.StringUtils;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@TapConnectorClass("spec_custom.json")
public class CustomConnector extends ConnectorBase {

    private static final String TAG = CustomConnector.class.getSimpleName();
    private static final ScriptFactory scriptFactory = InstanceFactory.instance(ScriptFactory.class, "engine"); //script factory
    private CustomConfig customConfig;
    private ScriptEngine initScriptEngine;
    private ConcurrentHashMap<String, ScriptEngine> writeEnginePool;

    private void initConnection(TapConnectionContext connectorContext) throws ScriptException {
        customConfig = new CustomConfig().load(connectorContext.getConnectionConfig());
        writeEnginePool = new ConcurrentHashMap<>(16);
        assert scriptFactory != null;
        initScriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT,
                new ScriptOptions().engineName(customConfig.getJsEngineName()).log(connectorContext.getLog()));
        initScriptEngine.eval(ScriptUtil.appendBeforeFunctionScript(customConfig.getCustomBeforeScript()) + "\n"
                + ScriptUtil.appendAfterFunctionScript(customConfig.getCustomAfterScript()));
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
            if (EmptyKit.isNotNull(customConfig) && customConfig.getCustomAfterOpr()) {
                ScriptUtil.executeScript(initScriptEngine, ScriptUtil.AFTER_FUNCTION_NAME);
            }
            if (initScriptEngine instanceof Closeable) {
                ((Closeable) initScriptEngine).close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
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
        connectorFunctions.supportCommandCallbackFunction(this::handleCommand);
    }

    private CommandResult handleCommand(final TapConnectionContext tapConnectionContext, final CommandInfo commandInfo) {
        final CollectLog logger = new CollectLog(tapConnectionContext.getLog());
        final String command = commandInfo.getCommand();
        logger.info("Start executing command [{}] ", command);
        CommandResult commandResult = new CommandResult();
        if (StringUtils.equals(command, "testRun")) {
            Object data = testRun(tapConnectionContext, commandInfo, logger);
            commandResult.setData(data);
        } else {
            logger.error("Unsupported command [{}]", command);
            commandResult.setData(logger.getLogs());
        }
        logger.info("Command [{}] execution complete.", command);

        return commandResult;
    }

    private Object testRun(TapConnectionContext tapConnectionContext, CommandInfo commandInfo, CollectLog logger) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Map<String, Object> argMap = commandInfo.getArgMap();
        String threadName = "CustomConnector-Test-Runner";
        TapConnectionContext newTapConnectionContext = new TapConnectionContext(tapConnectionContext.getSpecification(),
                DataMap.create(commandInfo.getConnectionConfig()), DataMap.create(commandInfo.getNodeConfig()), logger);
        Runnable runnable = () -> {
            Thread.currentThread().setName(threadName);
            try {
                init(newTapConnectionContext);
                String tableName = (String) commandInfo.getConnectionConfig().get("collectionName");
                String type = commandInfo.getType();
                TapTable tapTable = new TapTable(tableName);
                TapConnectorContext tapConnectorContext = new TapConnectorContext(tapConnectionContext.getSpecification(),
                        tapConnectionContext.getConnectionConfig(), tapConnectionContext.getNodeConfig(), logger);
                if (StringUtils.equals(type, "source")) {
                    logger.info("Start fetching data as a source......");
                    String action = commandInfo.getAction();
                    if (StringUtils.contains(action, "initial_sync")) {
                        logger.info("Start initializing sync......");
                        batchRead(tapConnectorContext, tapTable, new Object(), 1, (events, offsetObject) -> {
                            logger.info("Execute initial sync, get the data: {}", toTapEventStr(events, offsetObject, true));
                        });
                        logger.info("Initial sync complete.");
                    }
                    if (StringUtils.contains(action, "cdc")) {
                        logger.info("Start cdc sync......");
                        streamRead(tapConnectorContext, Collections.singletonList(tableName), new Object(), 1, StreamReadConsumer.create((events, offsetObject) -> {
                            logger.info("Execute cdc, get the data: {}", toTapEventStr(events, offsetObject, true));
                        }));
                        logger.info("Cdc sync complete.");
                    }
                    logger.info("Obtaining data as a source is complete.");

                } else if (StringUtils.equals(type, "target")) {
                    logger.info("Start processing data as a target......");
                    List<TapRecordEvent> tapRecordEvents = getTapRecordEvents((List<Map<String, Object>>) argMap.get("input"));
                    if (tapRecordEvents.size() == 0) {
                        logger.warn("The input is empty and cannot be processed");
                    } else {
                        logger.info("Processing data, input: \n{}", toTapEventStr(tapRecordEvents, null, true));
                        writeRecord(tapConnectorContext, tapRecordEvents, tapTable, writeListResult -> {
                            logger.info("Processing data, output: {}", toWriteListResultStr(writeListResult));
                        });
                    }
                    logger.info("Process data completion as target.");
                }
            } catch (ScriptException e) {
                logger.error( "{} execute script error:\n {}", TAG, e);
            } catch (Throwable throwable) {
                logger.error( "{} execute command error:\n {}", TAG, throwable);
            } finally {
                try {
                    stop(newTapConnectionContext);
                } catch (Throwable e) {
                    logger.error("{} stop error {}", TAG, e);
                }
                countDownLatch.countDown();
            }
        };

        Thread thread = null;
        try {
            thread = new Thread(runnable);
            thread.start();
            Integer timeout = (Integer) argMap.get("timeout");
            if (timeout == null || timeout <= 0) {
                timeout = 10;
            }
            if (timeout > 60) {
                timeout = 60;
            }
            boolean threadFinished = countDownLatch.await(timeout, TimeUnit.SECONDS);
            if (!threadFinished) {
                logger.warn("Execution has timed out and will terminate.");
                stop(newTapConnectionContext);
            }
        } catch (InterruptedException e) {
            logger.error("Thread [{}] interrupted, {}", threadName, e);
        } catch (Throwable throwable) {
            logger.error("[{}] execution failure， {}", throwable);
        } finally {
            if (thread != null && thread.isAlive()) {
                thread.stop();
            }
        }

        return logger.getLogs();
    }

    private String toWriteListResultStr(WriteListResult<TapRecordEvent> writeListResult) {
        StringBuilder sb = new StringBuilder("\n");
        sb.append("\tSuccessfully processed: ").append("i=").append(writeListResult.getInsertedCount()).append(", ")
                .append("u=").append(writeListResult.getModifiedCount()).append(", ")
                .append("d=").append(writeListResult.getRemovedCount()).append("\n");
        Map<TapRecordEvent, Throwable> errorMap = writeListResult.getErrorMap();
        if (errorMap != null && errorMap.size() != 0) {
            Map<String, Object> map = new HashMap<>();
            for (Map.Entry<TapRecordEvent, Throwable> entry : errorMap.entrySet()) {
                map.put("Record", entry.getKey());
                map.put("Error", entry.getValue());
            }
            sb.append("\tError Record: \n").append(toJson(map, JsonParser.ToJsonFeature.PrettyFormat));
        }
        return sb.toString();
    }


    private String toTapEventStr(List<? extends TapEvent> events, Object offsetObject, boolean printNum) {
        Map<String, Object> map = new HashMap<>();
        map.put("events", events);
        map.put("offset", offsetObject);
        return toJson(map, JsonParser.ToJsonFeature.PrettyFormat);
    }

    private List<TapRecordEvent> getTapRecordEvents(List<Map<String, Object>> maps) {
        List<TapRecordEvent> tapRecordEvents = new ArrayList<>();
        for (Map<String, Object> map : maps) {
            String op = (String) map.get("op");
            Integer type = (Integer) map.get("type");
            if (StringUtils.isNotEmpty(op)) {
                switch (op) {
                    case "i":
                        tapRecordEvents.add(TapInsertRecordEvent.create().init().table((String) map.get("table")).after((Map<String, Object>) map.get("after")));
                        break;
                    case "u":
                        tapRecordEvents.add(TapUpdateRecordEvent.create().init().table((String) map.get("table"))
                                .after((Map<String, Object>) map.get("after")).before((Map<String, Object>) map.get("before")));
                        break;
                    case "d":
                        tapRecordEvents.add(TapDeleteRecordEvent.create().init().table((String) map.get("table")).before((Map<String, Object>) map.get("before")));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported op: " + op);
                }
            } else if (type != null) {
                switch (type) {
                    case 300:
                        tapRecordEvents.add(fromJson(toJson(map), TapInsertRecordEvent.class));
                        break;
                    case 301:
                        tapRecordEvents.add(fromJson(toJson(map), TapDeleteRecordEvent.class));
                        break;
                    case 302:
                        tapRecordEvents.add(fromJson(toJson(map), TapUpdateRecordEvent.class));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported type: " + type);
                }
            }
        }
        return tapRecordEvents;
    }
    
    private static class CollectLog implements Log {

        private final Log logger;

        private final List<LogRecord> logRecords = new LinkedList<CollectLog.LogRecord>() {

            private static final int MAX_SIZE = 100;
            private boolean overflow = false;

            @Override
            public boolean add(CollectLog.LogRecord o) {
                if (overflow) {
                    return true;
                }
                if (size() > MAX_SIZE) {
                    this.overflow = true;
                    return super.add(new CollectLog.LogRecord("ERROR",
                            "The log exceeds the maximum limit, ignore the following logs.", System.currentTimeMillis()));
                }
                return super.add(o);
            }
        };

        public CollectLog(Log log) {
            this.logger = log;
        }

        public List<LogRecord> getLogs() {
            return logRecords;
        }

        @Override
        public void debug(String message, Object... params) {
            logger.debug(message, params);
            logRecords.add(new LogRecord("DEBUG", FormatUtils.format(message, params), System.currentTimeMillis()));
        }

        @Override
        public void info(String message, Object... params) {
            logger.info(message, params);
            logRecords.add(new LogRecord("INFO", FormatUtils.format(message, params), System.currentTimeMillis()));
        }

        @Override
        public void warn(String message, Object... params) {
            logger.warn(message, params);
            logRecords.add(new LogRecord("WARN", FormatUtils.format(message, params), System.currentTimeMillis()));
        }

        @Override
        public void error(String message, Object... params) {
            logger.error(message, params);
            logRecords.add(new LogRecord("ERROR", FormatUtils.format(message, params), System.currentTimeMillis()));
        }

        @Override
        public void error(String message, Throwable throwable) {
            logger.error(message, throwable);
            logRecords.add(new LogRecord("ERROR", FormatUtils.format(message, throwable), System.currentTimeMillis()));
        }

        @Override
        public void fatal(String message, Object... params) {
            logger.fatal(message, params);
            logRecords.add(new LogRecord("FATAL", FormatUtils.format(message, params), System.currentTimeMillis()));
        }

        private static class LogRecord {
            private final String level;
            private final String message;

            private final Long timestamp;

            private LogRecord(String level, String message, Long timestamp) {
                this.level = level;
                this.message = message;
                this.timestamp = timestamp;
            }

            public String getLevel() {
                return level;
            }

            public String getMessage() {
                return message;
            }

            public Long getTimestamp() {
                return timestamp;
            }
        }
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
            TestItem testItem = customTest.testBuildSchema();
            consumer.accept(testItem);
            if (TestItem.RESULT_SUCCESSFULLY == testItem.getResult()) {
                consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY));
            }
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
            scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName(customConfig.getJsEngineName()).log(connectorContext.getLog()));
            scriptEngine.eval(ScriptUtil.appendTargetFunctionScript(customConfig.getTargetScript()));
            writeEnginePool.put(threadName, scriptEngine);
        }
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
            try {
                ScriptUtil.executeScript(scriptEngine, ScriptUtil.TARGET_FUNCTION_NAME, new ArrayList<Map<String, Object>>() {{
                    add(temp);
                }});
                result.insertedCount(insert.get()).modifiedCount(update.get()).removedCount(delete.get());
            } catch (Exception e) {
                result.addError(event, e);
                break;
            }
        }
        writeListResultConsumer.accept(result);
    }

    private long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) {
        return 0;
    }

    private void batchRead(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws ScriptException {
        ScriptCore scriptCore = new ScriptCore(tapTable.getId());
        assert scriptFactory != null;
        ScriptEngine scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName(customConfig.getJsEngineName()).log(tapConnectorContext.getLog()));
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
    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        ScriptCore scriptCore = new ScriptCore(tableList.get(0));
        AtomicReference<Object> contextMap = new AtomicReference<>(offsetState);
        assert scriptFactory != null;
        ScriptEngine scriptEngine = scriptFactory.create(ScriptFactory.TYPE_JAVASCRIPT, new ScriptOptions().engineName(customConfig.getJsEngineName()).log(nodeContext.getLog()));
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
    }

    private Object timestampToStreamOffset(TapConnectorContext connectorContext, Long offsetStartTime) {
        return new HashMap<>();
    }

}
