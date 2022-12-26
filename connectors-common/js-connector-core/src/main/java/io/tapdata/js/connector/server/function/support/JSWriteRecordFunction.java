package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;

import java.util.*;


public class JSWriteRecordFunction extends FunctionBase implements FunctionSupport<WriteRecordFunction> {
    JSWriteRecordFunction(){
        super();
        super.functionName = JSFunctionNames.WriteRecordFunction;
    }
    @Override
    public WriteRecordFunction function(LoadJavaScripter javaScripter) {
        if (super.hasNotSupport(javaScripter)) return null;
        return (context,events,table,consumer)->{
            if (Objects.isNull(context)){
                throw new CoreException("TapConnectorContext must not be null or not be empty.");
            }
            if(Objects.isNull(table)){
                throw new CoreException("Table lists must not be null or not be empty.");
            }
            super.javaScripter.invoker(
                    JSFunctionNames.WriteRecordFunction.jsName(),
                    context.getConfigContext(),
                    context.getNodeConfig(),
                    table,
                    this.machiningEvents(events),
                    consumer
            );
        };
    }

    public static final String EVENT_TYPE_KEY = "event_type";
    public static final String EVENT_DATA_KEY = "event_data";
    public static final String EVENT_INSERT_KEY = "INSERT";
    public static final String EVENT_UPDATE_KEY = "UPDATE";
    public static final String EVENT_DELETE_KEY = "DELETE";
    private List<Map<String,Object>> machiningEvents(List<TapRecordEvent> tapRecordEvents){
        List<Map<String,Object>> events = new ArrayList<>();
        if (Objects.isNull(tapRecordEvents)) return events;
        tapRecordEvents.stream().filter(Objects::nonNull).forEach(tapRecord -> {
            Map<String,Object> event = new HashMap<>();
            if (tapRecord instanceof TapInsertRecordEvent){
                event.put(EVENT_TYPE_KEY,EVENT_INSERT_KEY);
                event.put(EVENT_DATA_KEY,((TapInsertRecordEvent)tapRecord).getAfter());
            }else if(tapRecord instanceof TapUpdateRecordEvent){
                event.put(EVENT_TYPE_KEY,EVENT_UPDATE_KEY);
                event.put(EVENT_DATA_KEY,((TapUpdateRecordEvent)tapRecord).getAfter());
            }else if(tapRecord instanceof TapDeleteRecordEvent){
                event.put(EVENT_TYPE_KEY,EVENT_DELETE_KEY);
                event.put(EVENT_DATA_KEY,((TapDeleteRecordEvent)tapRecord).getBefore());
            }
            events.add(event);
        });
        return events;
    }

    public static WriteRecordFunction create(LoadJavaScripter loadJavaScripter) {
        return new JSWriteRecordFunction().function(loadJavaScripter);
    }
}
