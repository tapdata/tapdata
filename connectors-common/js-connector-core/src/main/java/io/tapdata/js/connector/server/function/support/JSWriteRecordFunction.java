package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.js.connector.JSConnector;
import io.tapdata.js.connector.base.EventTag;
import io.tapdata.js.connector.base.EventType;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;
import io.tapdata.write.WriteValve;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;


public class JSWriteRecordFunction extends FunctionBase implements FunctionSupport<WriteRecordFunction> {
    AtomicBoolean isAlive = new AtomicBoolean(true);

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

    private ConcurrentHashMap<String, ScriptEngine> writeEnginePool = new ConcurrentHashMap<>(16);

    private synchronized void write(TapConnectorContext context, List<TapRecordEvent> tapRecordEvents, TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws ScriptException {
        if (Objects.isNull(context)) {
            throw new CoreException("TapConnectorContext cannot not be empty.");
        }
        if (Objects.isNull(table)) {
            throw new CoreException("Table lists cannot not be empty.");
        }
        String threadName = Thread.currentThread().getName();
        ScriptEngine scriptEngine;
        if (writeEnginePool.containsKey(threadName)) {
            scriptEngine = writeEnginePool.get(threadName);
        } else {
            scriptEngine = javaScripter.scriptEngine();
            writeEnginePool.put(threadName, scriptEngine);
        }

        AtomicLong insert = new AtomicLong(0);
        AtomicLong update = new AtomicLong(0);
        AtomicLong delete = new AtomicLong(0);

        List<Map<String, Object>> machiningEvents = this.machiningEvents(tapRecordEvents, table.getId());

        WriteListResult<TapRecordEvent> result = new WriteListResult<>();
        if (!this.doSubFunctionNotSupported()) {
            Map<String, List<Map<String, Object>>> machiningEventsGroupAfter = machiningEvents.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(map -> String.valueOf(map.get(EventTag.EVENT_TYPE))));
            this.exec(context,machiningEventsGroupAfter.get(EventType.insert),JSFunctionNames.InsertRecordFunction,insert,update,delete);
            this.exec(context,machiningEventsGroupAfter.get(EventType.update),JSFunctionNames.UpdateRecordFunction,insert,update,delete);
            this.exec(context, machiningEventsGroupAfter.get(EventType.delete),JSFunctionNames.DeleteRecordFunction,insert,update,delete);
        }
        this.exec(context,machiningEvents,JSFunctionNames.WriteRecordFunction,insert,update,delete);
        writeListResultConsumer.accept(result.insertedCount(insert.get()).modifiedCount(update.get()).removedCount(delete.get()));
    }

    private void exec(TapConnectorContext context, List<Map<String, Object>> execData, JSFunctionNames function,AtomicLong insert, AtomicLong update, AtomicLong delete ) {
        if (!this.doNotSupport(function)) {
            try {
                Object invoker;
                synchronized (JSConnector.execLock) {
                    invoker = super.javaScripter.invoker(
                            JSFunctionNames.InsertRecordFunction.jsName(),
                            Optional.ofNullable(context.getConnectionConfig()).orElse(new DataMap()),
                            Optional.ofNullable(context.getNodeConfig()).orElse(new DataMap()),
                            execData
                    );
                }
                try {
                    List<Map<String,Object>> succeedData = (List<Map<String, Object>>) invoker;
                    Map<String, List<Map<String, Object>>> stringListMap = succeedData.stream().filter(Objects::nonNull).collect(Collectors.groupingBy(map -> String.valueOf(map.get(EventTag.EVENT_TYPE))));
                    List<Map<String, Object>> insertData = stringListMap.get(EventType.insert);
                    List<Map<String, Object>> updateData = stringListMap.get(EventType.update);
                    List<Map<String, Object>> deleteData = stringListMap.get(EventType.delete);
                    insert.addAndGet(Objects.isNull(insertData)?0:insertData.size());
                    update.addAndGet(Objects.isNull(updateData)?0:updateData.size());
                    delete.addAndGet(Objects.isNull(deleteData)?0:deleteData.size());
                }catch (Exception ignored){}
            } catch (Exception e) {
                throw new CoreException(String.format("Exceptions occurred when executing %s to write data. The operations of adding %s, modifying %s, and deleting %s failed,msg: %s.", function.jsName(), insert.get(), update.get(), delete.get(), e.getMessage()));
            }
        }
    }

    private List<Map<String, Object>> machiningEvents(List<TapRecordEvent> tapRecordEvents, final String tableId) {
        List<Map<String, Object>> events = new ArrayList<>();
        if (Objects.isNull(tapRecordEvents)) return events;
        tapRecordEvents.stream().filter(Objects::nonNull).forEach(tapRecord -> {
            Map<String, Object> event = new HashMap<>();
            if (tapRecord instanceof TapInsertRecordEvent) {
                event.put(EventTag.EVENT_TYPE, EventType.insert);
                event.put(EventTag.AFTER_DATA, ((TapInsertRecordEvent) tapRecord).getAfter());
                //insert.incrementAndGet();
            } else if (tapRecord instanceof TapUpdateRecordEvent) {
                event.put(EventTag.EVENT_TYPE, EventType.update);
                event.put(EventTag.BEFORE_DATA, ((TapUpdateRecordEvent) tapRecord).getBefore());
                event.put(EventTag.AFTER_DATA, ((TapUpdateRecordEvent) tapRecord).getAfter());
                //update.incrementAndGet();
            } else if (tapRecord instanceof TapDeleteRecordEvent) {
                event.put(EventTag.EVENT_TYPE, EventType.delete);
                event.put(EventTag.BEFORE_DATA, ((TapDeleteRecordEvent) tapRecord).getBefore());
                //delete.incrementAndGet();
            }
            event.put(EventTag.REFERENCE_TIME, tapRecord.getReferenceTime());
            event.put(EventTag.TABLE_NAME, tableId);
            events.add(event);
        });
        return events;
    }

    public static JSWriteRecordFunction create(AtomicBoolean isAlive) {
        return new JSWriteRecordFunction().isAlive(isAlive);
    }

    public WriteRecordFunction write(LoadJavaScripter loadJavaScripter) {
        return this.function(loadJavaScripter);
    }
}
