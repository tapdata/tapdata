package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.js.connector.JSConnector;
import io.tapdata.js.connector.iengine.LoadJavaScripter;
import io.tapdata.js.connector.server.function.FunctionBase;
import io.tapdata.js.connector.server.function.FunctionSupport;
import io.tapdata.js.connector.server.function.JSFunctionNames;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.source.RawDataCallbackFilterFunctionV2;

import java.util.*;
import java.util.stream.Collectors;

import static io.tapdata.entity.simplify.TapSimplify.list;
import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class JSRawDataCallbackFunction extends FunctionBase implements FunctionSupport<RawDataCallbackFilterFunctionV2> {
    private static final String TAG = JSRawDataCallbackFunction.class.getSimpleName();

    private JSRawDataCallbackFunction() {
        super();
        super.functionName = JSFunctionNames.WebHookFunction;
    }

    @Override
    public RawDataCallbackFilterFunctionV2 function(LoadJavaScripter javaScripter) {
        if (super.hasNotSupport(javaScripter)) return null;
        return this::webHookEvent;
    }

    /**
     * [
     * {
     * "eventType" : "I",                    //I U D
     * "tableName" : "example_table_name",  //table name
     * "beforeData":{},
     * "afterData":{}
     * },
     * {
     * "eventType" : "U",                    //I U D
     * "tableName" : "example_table_name",  //table name
     * "beforeData":{},
     * "afterData":{}
     * }
     * ]
     */
    private final io.tapdata.js.connector.base.EventType eventType = new io.tapdata.js.connector.base.EventType();
    private Integer dataIndex = new Integer(1);

    private List<TapEvent> webHookEvent(TapConnectorContext context, List<String> tableNameList, Map<String, Object> dataMap) {
        this.javaScripter.scriptEngine().put("eventType", this.eventType);
        try {
            Object invoker;
            synchronized (JSConnector.execLock) {
                invoker = super.javaScripter.invoker(
                        this.functionName.jsName(),
                        Optional.ofNullable(context.getConnectionConfig()).orElse(new DataMap()),
                        Optional.ofNullable(context.getNodeConfig()).orElse(new DataMap()),
                        Optional.ofNullable(tableNameList).orElse(new ArrayList<>()),
                        Optional.ofNullable(dataMap).orElse(new HashMap<>())
                );
            }
            this.dataIndex = 1;
            if (invoker instanceof Collection) {
                Collection<Object> eventList = (Collection<Object>) invoker;
                return eventList.stream()
                        .filter(Objects::nonNull)
                        .map(m -> {
                            TapEvent event = this.eventType.setEvent(m, this.dataIndex, TAG);
                            this.dataIndex++;
                            return event;
                        })
                        .collect(Collectors.toList());
            } else if (invoker instanceof Map) {
                return list(this.eventType.setEvent(invoker, this.dataIndex, TAG));
            } else {
                throw new CoreException("Method '" + this.functionName.jsName() + "' failed to execute. Unable to get the return result. The final result will be null.");
            }
        } catch (Exception e) {
            TapLogger.warn(TAG, " Method " + this.functionName.jsName() + " failed to execute. message: " + e.getMessage() + "\nevent data: " + toJson(dataMap));
        }
        return null;
    }


    public static RawDataCallbackFilterFunctionV2 create(LoadJavaScripter loadJavaScripter) {
        return new JSRawDataCallbackFunction().function(loadJavaScripter);
    }
}
