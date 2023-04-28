package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.js.connector.JSConnector;
import io.tapdata.js.connector.base.EventTag;
import io.tapdata.js.connector.base.EventType;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.js.connector.server.function.base.SchemaAccept;
import io.tapdata.js.connector.server.sender.WriteRecordRender;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;

import javax.script.ScriptException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;


public class JSWriteRecordFunction extends FunctionBase implements FunctionSupport<WriteRecordFunction> {
    public static final String TAG = JSWriteRecordFunction.class.getSimpleName();
    AtomicBoolean isAlive = new AtomicBoolean(true);

    Map<Integer, Object> writeCache = new ConcurrentHashMap<>();

    public JSWriteRecordFunction isAlive(AtomicBoolean isAlive) {
        this.isAlive = isAlive;
        return this;
    }

    JSWriteRecordFunction() {
        super();
        super.functionName = JSFunctionNames.WriteRecordFunction;
    }

    @Override
    public WriteRecordFunction function(LoadJavaScripter javaScripter) {
        if (super.hasNotSupport(javaScripter) && this.doSubFunctionNotSupported()) return null;
        return this::write;
    }

    private boolean doSubFunctionNotSupported() {
        return this.doNotSupport(JSFunctionNames.InsertRecordFunction)
                && this.doNotSupport(JSFunctionNames.DeleteRecordFunction)
                && this.doNotSupport(JSFunctionNames.UpdateRecordFunction);
    }

    private boolean doNotSupport(JSFunctionNames function) {
        return !this.javaScripter.functioned(function.jsName());
    }

    private synchronized void write(TapConnectorContext context, List<TapRecordEvent> tapRecordEvents, TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws ScriptException {
        if (Objects.isNull(context)) {
            throw new CoreException("TapConnectorContext cannot not be empty.");
        }
        if (Objects.isNull(table)) {
            throw new CoreException("Table lists cannot not be empty.");
        }
        List<Map<String, Object>> machiningEvents = this.machiningEvents(tapRecordEvents, table.getId());
        String tableJsonString = new SchemaAccept().tableMap(table);

        if (!this.doSubFunctionNotSupported()) {
            String cacheEventType = null;
            List<Map<String, Object>> execData = new ArrayList<>();
            for (Map<String, Object> event : machiningEvents) {
                String cacheEventTypeTemp = String.valueOf(Optional.ofNullable(event.get(EventTag.EVENT_TYPE)).orElse(EventType.insert));
                if (Objects.isNull(cacheEventType) || !cacheEventType.equals(cacheEventTypeTemp)) {
                    if (!execData.isEmpty()) {
                        this.execDrop(cacheEventType, context, execData, writeListResultConsumer, tableJsonString);
                        execData = new ArrayList<>();
                    }
                    cacheEventType = cacheEventTypeTemp;
                }
                execData.add(event);
            }
            if (!execData.isEmpty()) {
                this.execDrop(cacheEventType, context, execData, writeListResultConsumer, tableJsonString);
            }
        }
        this.exec(context, machiningEvents, JSFunctionNames.WriteRecordFunction, writeListResultConsumer, tableJsonString);
        //js执行出错不需要清除缓存，重试时需要使用
        this.writeCache = new ConcurrentHashMap<>();
    }

    private void execDrop(String cacheEventType, TapConnectorContext context, List<Map<String, Object>> execData,Consumer<WriteListResult<TapRecordEvent>> consumer, String tableJsonString){
        JSFunctionNames functionName = cacheEventType.equals(EventType.insert) ? JSFunctionNames.InsertRecordFunction : (cacheEventType.equals(EventType.update) ? JSFunctionNames.UpdateRecordFunction : JSFunctionNames.DeleteRecordFunction);
        for (Map<String, Object> execDatum : execData) {
            this.exec(context, execDatum, functionName, consumer, tableJsonString);
        }
    }

    private void exec(TapConnectorContext context, Object execData, JSFunctionNames function,Consumer<WriteListResult<TapRecordEvent>> consumer, String tableJsonString) {
        if (!this.doNotSupport(function)) {
            WriteRecordRender writeResultCollector= data -> {
                AtomicLong insert = new AtomicLong();
                AtomicLong update = new AtomicLong();
                AtomicLong delete = new AtomicLong();
                if (Objects.nonNull(data)) {
                    if (data instanceof Map) {
                        List<Object> dataList = new ArrayList<>();
                        dataList.add(data);
                        this.convertData(dataList, insert, update, delete);
                    } else if (data instanceof Collection) {
                        this.convertData(data, insert, update, delete);
                    } else if (data.getClass().isArray()) {
                        Object[] dataArr = (Object[]) data;
                        List<Object> list = new ArrayList<>(Arrays.asList(dataArr));
                        this.convertData(list, insert, update, delete);
                    } else {
                        this.convertData(data, insert, update, delete);
                    }
                    WriteListResult<TapRecordEvent> result = new WriteListResult<>();
                    consumer.accept(result.insertedCount(insert.get()).modifiedCount(update.get()).removedCount(delete.get()));
                }
            };
            try {
                boolean isWriteRecord = JSFunctionNames.WriteRecordFunction.jsName().equals(function.jsName());
                Object invoker;
                synchronized (JSConnector.execLock) {
                    invoker = super.javaScripter.invoker(
                            function.jsName(),
                            Optional.ofNullable(context.getConnectionConfig()).orElse(new DataMap()),
                            Optional.ofNullable(context.getNodeConfig()).orElse(new DataMap()),
                            execData,
                            isWriteRecord ? writeResultCollector : tableJsonString,
                            isWriteRecord ? tableJsonString : null
                    );
                }
                if (!isWriteRecord) {
                    boolean isNotIgnore;
                    try {
                        isNotIgnore = Objects.isNull(invoker) || (invoker instanceof Boolean ? (Boolean) invoker : Boolean.parseBoolean(String.valueOf(invoker)));
                    } catch (Exception e) {
                        isNotIgnore = true;
                    }
                    if (isNotIgnore) {
                        List<Object> list = new ArrayList<>();
                        if (execData instanceof Collection) {
                            list.addAll((Collection<?>) execData);
                        } else {
                            list.add(execData);
                        }
                        AtomicLong insert = new AtomicLong();
                        AtomicLong update = new AtomicLong();
                        AtomicLong delete = new AtomicLong();
                        this.convertData(list, insert, update, delete);
                        WriteListResult<TapRecordEvent> result = new WriteListResult<>();
                        consumer.accept(result.insertedCount(insert.get()).modifiedCount(update.get()).removedCount(delete.get()));
                    }
                }
            } catch (Exception e) {
                throw new CoreException(String.format("Exceptions occurred when executing %s to write data failed, msg: %s.", function.jsName(), e.getMessage()));
            }
        }
    }

    private void convertData(Object invoker, AtomicLong insert, AtomicLong update, AtomicLong delete) {
        List<Map<String, Object>> succeedData = new ArrayList<>();
        try {
            succeedData = (List<Map<String, Object>>) invoker;
        } catch (Exception ignored) {
            TapLogger.warn(TAG, "No normal JavaScript writing return value was received. Please keep the return value structure which is include the following map or list in line with the format requirements: \n" +
                    "{\"eventType\":\"i/u/d\",\"afterData\":{},\"beforeData\":{},\"tableName\":\"\",\"referenceTime\":\"\"}");
        }
        Map<String, List<Map<String, Object>>> stringListMap = succeedData.stream().filter(ent->{
            if (Objects.nonNull(ent)){
                Object eventType = ent.get(EventTag.EVENT_TYPE);
                String type = eventType instanceof String ? (String)eventType : String.valueOf(eventType);
                Object data = EventType.delete.equals(type) ? ent.get(EventTag.BEFORE_DATA) : ent.get(EventTag.AFTER_DATA);
                Integer code = (Optional.ofNullable(data).orElse(new HashMap<>())).hashCode();
                this.writeCache.put(code, ent);
                return true;
            }
            return false;
        }).collect(Collectors.groupingBy(map -> String.valueOf(map.get(EventTag.EVENT_TYPE))));

        List<Map<String, Object>> insertData = stringListMap.get(EventType.insert);
        List<Map<String, Object>> updateData = stringListMap.get(EventType.update);
        List<Map<String, Object>> deleteData = stringListMap.get(EventType.delete);
        insert.addAndGet(Objects.isNull(insertData) ? 0 : insertData.size());
        update.addAndGet(Objects.isNull(updateData) ? 0 : updateData.size());
        delete.addAndGet(Objects.isNull(deleteData) ? 0 : deleteData.size());
    }

    private List<Map<String, Object>> machiningEvents(List<TapRecordEvent> tapRecordEvents, final String tableId) {
        List<Map<String, Object>> events = new ArrayList<>();
        if (Objects.isNull(tapRecordEvents)) return events;
        tapRecordEvents.stream().filter(Objects::nonNull).forEach(tapRecord -> {
            Map<String, Object> event = new HashMap<>();
            if (tapRecord instanceof TapInsertRecordEvent) {
                Map<String, Object> after = ((TapInsertRecordEvent) tapRecord).getAfter();
                if (this.hasCached(after)) {
                    return;
                }
                event.put(EventTag.EVENT_TYPE, EventType.insert);
                event.put(EventTag.AFTER_DATA, after);
                //insert.incrementAndGet();
            } else if (tapRecord instanceof TapUpdateRecordEvent) {
                Map<String, Object> after = ((TapUpdateRecordEvent) tapRecord).getAfter();
                if (this.hasCached(after)) {
                    return;
                }
                event.put(EventTag.EVENT_TYPE, EventType.update);
                event.put(EventTag.BEFORE_DATA, ((TapUpdateRecordEvent) tapRecord).getBefore());
                event.put(EventTag.AFTER_DATA, after);
                //update.incrementAndGet();
            } else if (tapRecord instanceof TapDeleteRecordEvent) {
                Map<String, Object> before = ((TapDeleteRecordEvent) tapRecord).getBefore();
                if (this.hasCached(before)) {
                    return;
                }
                event.put(EventTag.EVENT_TYPE, EventType.delete);
                event.put(EventTag.BEFORE_DATA, before);
                //delete.incrementAndGet();
            }
            event.put(EventTag.REFERENCE_TIME, tapRecord.getReferenceTime());
            event.put(EventTag.TABLE_NAME, tableId);
            events.add(event);
        });
        return events;
    }

    private boolean hasCached(Map<String, Object> data) {
        Integer code = data.hashCode();
        return Objects.nonNull(this.writeCache.get(code));
    }

    public static JSWriteRecordFunction create(AtomicBoolean isAlive) {
        return new JSWriteRecordFunction().isAlive(isAlive);
    }

    public WriteRecordFunction write(LoadJavaScripter loadJavaScripter) {
        return this.function(loadJavaScripter);
    }

}
