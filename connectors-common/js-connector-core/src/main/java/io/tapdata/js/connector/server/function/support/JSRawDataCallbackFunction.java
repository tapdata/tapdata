package io.tapdata.js.connector.server.function.support;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.js.connector.JSConnector;
import io.tapdata.js.connector.base.EventTag;
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
     * "EVENT_TYPE" : "I",                    //I U D
     * "TABLE_NAME" : "example_table_name",  //table name
     * "DATA":{
     * ...
     * }
     * },
     * {
     * "EVENT_TYPE" : "U",                    //I U D
     * "TABLE_NAME" : "example_table_name",  //table name
     * "DATA":{
     * ...
     * }
     * }
     * ]
     */
    private final io.tapdata.js.connector.base.EventType eventType = new io.tapdata.js.connector.base.EventType();
    private int dataIndex = 1;

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
                        .map(this::setEvent)
                        .collect(Collectors.toList());
            } else if (invoker instanceof Map) {
                return list(this.setEvent(invoker));
            } else {
                throw new CoreException("Method '" + this.functionName.jsName() + "' failed to execute. Unable to get the return result. The final result will be null.");
            }
        } catch (Exception e) {
            TapLogger.warn(TAG, " Method " + this.functionName.jsName() + " failed to execute. message: " + e.getMessage() + "\nevent data: " + toJson(dataMap));
        }
        return null;
    }

    private TapEvent setEvent(Object eventDataFromJs) {
        if (eventDataFromJs instanceof Map) {
            Map<String, Object> result = (Map<String, Object>) eventDataFromJs;
            Object eventType = result.get(EventTag.EVENT_TYPE);
            if (Objects.isNull(eventType) || this.eventType.isEventType(String.valueOf(eventType))) {
                throw new CoreException("Article " + this.dataIndex + " Record: Please use EVENT_TYPE to indicate event type (i/u/d). ");
            }
            Object tableName = result.get(EventTag.TABLE_NAME);
            if (Objects.isNull(tableName) || "".equals(tableName)) {
                throw new CoreException("Article " + this.dataIndex + " Record: Please use TABLE_NAME to indicate table name. ");
            }
            Object after = result.get(EventTag.AFTER_DATA);
            if (Objects.isNull(after) && this.eventType.update.equals(eventType)) {
                throw new CoreException("Article " + this.dataIndex + " Record: An update data event was received, but not used AFTER_DATA describes the update data. ");
            }
            if (!(after instanceof Map)) {
                throw new CoreException("Article " + this.dataIndex + " Record: Wrong data representation, need to use k-v map to represent AFTER_DATA. ");
            }
            Object before = result.get(EventTag.BEFORE_DATA);
            if (Objects.isNull(before) && (this.eventType.insert.equals(eventType) || this.eventType.delete.equals(eventType))) {
                throw new CoreException(
                        this.eventType.insert.equals(eventType) ?
                                "Article " + this.dataIndex + " Record: insert event was received, but not used AFTER_DATA describes the insert data. " :
                                "Article " + this.dataIndex + " Record: delete event was received, but not used AFTER_DATA describes the delete data. "
                );
            }
            if (!(before instanceof Map)) {
                throw new CoreException("Article " + this.dataIndex + " Record: Wrong data representation, need to use k-v map to represent BEFORE_DATA. ");
            }
            Object referenceTimeObj = result.get(EventTag.REFERENCE_TIME);
            Long referenceTime = System.currentTimeMillis();
            if (Objects.isNull(referenceTimeObj) || !(referenceTimeObj instanceof Long)) {
                TapLogger.warn(TAG, "Article " + this.dataIndex + " Record: ");
            } else {
                referenceTime = Long.valueOf(String.valueOf(referenceTime));
            }
            this.dataIndex++;
            switch (String.valueOf(eventType)) {
                case "d":
                    return TapSimplify.deleteDMLEvent((Map<String, Object>) before, String.valueOf(tableName)).referenceTime(referenceTime);
                case "u":
                    return TapSimplify.updateDMLEvent((Map<String, Object>) before, (Map<String, Object>) after, String.valueOf(tableName)).referenceTime(referenceTime);
                default:
                    return TapSimplify.insertRecordEvent((Map<String, Object>) before, String.valueOf(tableName)).referenceTime(referenceTime);
            }
        } else {
            throw new CoreException("Article " + this.dataIndex + " Record:  The event format is incorrect. Please use the following rules to organize the returned results :\n" +
                    "{\n" +
                    "\"EVENT_TYPE\": String('i/u/d'),\n" +
                    " \"TABLE_NAME\": String('example_table_name'), " +
                    "\n\"BEFORE_DATA\": {}," +
                    "\n\"AFTER_DATA\": {}," +
                    "\n\"REFERENCE_TIME\": Number(time_stamp)" +
                    "}\n");
        }
    }

    public static RawDataCallbackFilterFunctionV2 create(LoadJavaScripter loadJavaScripter) {
        return new JSRawDataCallbackFunction().function(loadJavaScripter);
    }
}
