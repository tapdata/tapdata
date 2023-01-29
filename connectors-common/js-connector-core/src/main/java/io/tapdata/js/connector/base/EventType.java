package io.tapdata.js.connector.base;

import io.tapdata.entity.error.CoreException;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class EventType {
    public static final String insert = "i";
    public static final String update = "u";
    public static final String delete = "d";

    public Map<String, Object> generateEvent(String eventType, String tableName, Object data) {
        Map<String,Object> eventData = new HashMap<>();
        eventData.put("EVENT_TYPE", Optional.ofNullable(eventType).orElse(insert));
        if (Objects.isNull(tableName) || "".equals(tableName.trim())){
            throw new CoreException(" When generating event results, the data corresponding table name is empty, and generation failed.");
        }
        eventData.put("TABLE_NAME",tableName);
        if (Objects.isNull(data)){
            throw new CoreException(" When generating event results, the data is empty, and generation failed.");
        }
        eventData.put("DATA",data);
        return eventData;
    }
    public Map<String, Object> generateInsertEvent(String tableName, Object data) {
        return this.generateEvent(insert,tableName,data);
    }
    public Map<String, Object> generateUpdateEvent(String tableName, Object data) {
        return this.generateEvent(update,tableName,data);
    }
    public Map<String, Object> generateDeleteEvent(String tableName, Object data) {
        return this.generateEvent(delete,tableName,data);
    }
    public boolean isEventType(String eventType){
        return insert.equals(eventType) || update.equals(eventType) || delete.equals(eventType);
    }
}
