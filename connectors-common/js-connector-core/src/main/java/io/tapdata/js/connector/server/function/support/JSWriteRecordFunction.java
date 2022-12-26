package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;

import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;


public class JSWriteRecordFunction extends FunctionBase implements FunctionSupport<WriteRecordFunction> {
    AtomicBoolean isAlive = new AtomicBoolean(true);
    public JSWriteRecordFunction isAlive(AtomicBoolean isAlive){
        this.isAlive = isAlive;
        return this;
    }
    JSWriteRecordFunction(){
        super();
        super.functionName = JSFunctionNames.WriteRecordFunction;
    }
    @Override
    public WriteRecordFunction function(LoadJavaScripter javaScripter) {
        if (super.hasNotSupport(javaScripter)) return null;
        return this::write;
    }
    private ConcurrentHashMap<String, ScriptEngine> writeEnginePool = new ConcurrentHashMap<>(16);
    private void write(TapConnectorContext context, List<TapRecordEvent> tapRecordEvents, TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws ScriptException {
        if (Objects.isNull(context)){
            throw new CoreException("TapConnectorContext must not be null or not be empty.");
        }
        if(Objects.isNull(table)){
            throw new CoreException("Table lists must not be null or not be empty.");
        }
        String threadName = Thread.currentThread().getName();
        ScriptEngine scriptEngine ;
        if (writeEnginePool.containsKey(threadName)) {
            scriptEngine = writeEnginePool.get(threadName);
        } else {
            scriptEngine = javaScripter.scriptEngine();
            writeEnginePool.put(threadName, scriptEngine);
        }

        AtomicLong insert = new AtomicLong(0);
        AtomicLong update = new AtomicLong(0);
        AtomicLong delete = new AtomicLong(0);
        List<Map<String, Object>> machiningEvents = machiningEvents(tapRecordEvents,table.getId(), insert, update, delete);
        WriteListResult<TapRecordEvent> result = new WriteListResult<>();
        super.javaScripter.invoker(
                JSFunctionNames.WriteRecordFunction.jsName(),
                context.getConfigContext(),
                context.getNodeConfig(),
                machiningEvents
        );
        writeListResultConsumer.accept(result.insertedCount(insert.get()).modifiedCount(update.get()).removedCount(delete.get()));
    }

    public static final String EVENT_TYPE_KEY = "event_type";
    public static final String EVENT_TABLE_KEY = "event_table";
    public static final String EVENT_DATA_KEY = "event_data";
    public static final String EVENT_INSERT_KEY = "INSERT";
    public static final String EVENT_UPDATE_KEY = "UPDATE";
    public static final String EVENT_DELETE_KEY = "DELETE";
    private List<Map<String,Object>> machiningEvents(List<TapRecordEvent> tapRecordEvents,final String tableId,AtomicLong insert,AtomicLong update,AtomicLong delete){
        List<Map<String,Object>> events = new ArrayList<>();
        if (Objects.isNull(tapRecordEvents)) return events;
        tapRecordEvents.stream().filter(Objects::nonNull).forEach(tapRecord -> {
            Map<String,Object> event = new HashMap<>();
            if (tapRecord instanceof TapInsertRecordEvent){
                event.put(EVENT_TYPE_KEY,EVENT_INSERT_KEY);
                event.put(EVENT_DATA_KEY,((TapInsertRecordEvent)tapRecord).getAfter());
                insert.incrementAndGet();
            }else if(tapRecord instanceof TapUpdateRecordEvent){
                event.put(EVENT_TYPE_KEY,EVENT_UPDATE_KEY);
                event.put(EVENT_DATA_KEY,((TapUpdateRecordEvent)tapRecord).getAfter());
                update.incrementAndGet();
            }else if(tapRecord instanceof TapDeleteRecordEvent){
                event.put(EVENT_TYPE_KEY,EVENT_DELETE_KEY);
                event.put(EVENT_DATA_KEY,((TapDeleteRecordEvent)tapRecord).getBefore());
                delete.incrementAndGet();
            }
            event.put(EVENT_TABLE_KEY,tableId);
            events.add(event);
        });
        return events;
    }

    public static WriteRecordFunction create(LoadJavaScripter loadJavaScripter, AtomicBoolean isAlive) {
        return new JSWriteRecordFunction().isAlive(isAlive).function(loadJavaScripter);
    }
}
