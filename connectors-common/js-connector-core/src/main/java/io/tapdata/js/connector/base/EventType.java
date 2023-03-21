package io.tapdata.js.connector.base;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class EventType {
    public static final String insert = "i";
    public static final String update = "u";
    public static final String delete = "d";

    public Map<String, Object> generateEvent(String eventType, String tableName, Object data) {
        Map<String, Object> eventData = new HashMap<>();
        eventData.put(EventTag.EVENT_TYPE, Optional.ofNullable(eventType).orElse(insert));
        if (Objects.isNull(tableName) || "".equals(tableName.trim())) {
            throw new CoreException(" When generating event results, the data corresponding table name is empty, and generation failed.");
        }
        eventData.put(EventTag.TABLE_NAME, tableName);
        if (Objects.isNull(data)) {
            throw new CoreException(" When generating event results, the data is empty, and generation failed.");
        }
        eventData.put(EventTag.AFTER_DATA, data);
        return eventData;
    }

    public Map<String, Object> generateInsertEvent(String tableName, Object data) {
        return this.generateEvent(insert, tableName, data);
    }

    public Map<String, Object> generateUpdateEvent(String tableName, Object data) {
        return this.generateEvent(update, tableName, data);
    }

    public Map<String, Object> generateDeleteEvent(String tableName, Object data) {
        return this.generateEvent(delete, tableName, data);
    }

    public boolean isEventType(String eventType) {
        return insert.equals(eventType) || update.equals(eventType) || delete.equals(eventType);
    }

    public TapEvent setEvent(Object eventDataFromJs, Integer dataIndex, String tag) {
        if (eventDataFromJs instanceof Map) {
            Map<String, Object> result = (Map<String, Object>) eventDataFromJs;
            Object eventType = result.get(EventTag.EVENT_TYPE);
            if (Objects.isNull(eventType) || !this.isEventType(String.valueOf(eventType))) {
                throw new CoreException("Article " + dataIndex + " Record: Please use eventType to indicate event type (i/u/d). ");
            }
            Object tableName = result.get(EventTag.TABLE_NAME);
            if (Objects.isNull(tableName) || "".equals(tableName)) {
                throw new CoreException("Article " + dataIndex + " Record: Please use tableName to indicate table name. ");
            }
            Object after = result.get(EventTag.AFTER_DATA);
            if (Objects.isNull(after) && (update.equals(eventType) || insert.equals(eventType))) {
                throw new CoreException(
                        insert.equals(eventType) ?
                                "Article " + dataIndex + " Record: insert event was received, but not used afterData describes the insert data. "
                                : "Article " + dataIndex + " Record: An update data event was received, but not used afterData describes the update data. ");
            }
            if (!(after instanceof Map) && (update.equals(eventType) || insert.equals(eventType))) {
                throw new CoreException("Article " + dataIndex + " Record: Wrong data representation, need to use k-v map to represent afterData. ");
            }
            Object before = result.get(EventTag.BEFORE_DATA);
            if (Objects.isNull(before) && delete.equals(eventType)) {
                throw new CoreException("Article " + dataIndex + " Record: delete event was received, but not used beforeData describes the delete data. ");
            }
            if (!(before instanceof Map) && delete.equals(eventType)) {
                throw new CoreException("Article " + dataIndex + " Record: Wrong data representation, need to use k-v map to represent beforeData. ");
            }
            Object referenceTimeObj = result.get(EventTag.REFERENCE_TIME);
            Long referenceTime = System.currentTimeMillis();
            if (Objects.isNull(referenceTimeObj)) {
                TapLogger.warn(tag, "Article " + dataIndex + " Record: cannot find referenceTime, and set now time for referenceTime.");
            } else {
                boolean casted = false;
                if (referenceTimeObj instanceof Number) {
                    referenceTime = ((Number) referenceTimeObj).longValue();
                    casted = true;
                } else if (referenceTimeObj instanceof String) {
                    try {
                        referenceTime = Long.parseLong((String) referenceTimeObj);
                        casted = true;
                    } catch (Exception ignored) {
                    }
                }
                if (!casted) {
                    TapLogger.warn(tag, "Article " + dataIndex + " Record: cannot find referenceTime, and set now time for referenceTime.");
                }
            }
            switch (String.valueOf(eventType)) {
                case "d":
                    return TapSimplify.deleteDMLEvent((Map<String, Object>) before, String.valueOf(tableName)).referenceTime(referenceTime);
                case "u":
                    return TapSimplify.updateDMLEvent((Map<String, Object>) before, (Map<String, Object>) after, String.valueOf(tableName)).referenceTime(referenceTime);
                default:
                    return TapSimplify.insertRecordEvent((Map<String, Object>) after, String.valueOf(tableName)).referenceTime(referenceTime);
            }
        } else {
            throw new CoreException("Article " + dataIndex + " Record:  The event format is incorrect. Please use the following rules to organize the returned results :\n" +
                    "{\n" +
                    "\"eventType\": String('i/u/d'),\n" +
                    " \"tableName\": String('exampleTableName'), " +
                    "\n\"beforeData\": {}," +
                    "\n\"afterData\": {}," +
                    "\n\"referenceTime\": Number(timeStamp)" +
                    "}\n");
        }
    }

    public static Map<String, Object> defaultEventData(Object dataMap, String tableName) {
        if (Objects.isNull(dataMap)) return null;
        Map<String, Object> data = new HashMap<>();
        if (dataMap instanceof Map) {
            Map<String, Object> result = (Map<String, Object>) dataMap;
            Object eventType = result.get(EventTag.EVENT_TYPE);
            Object tableNameObj = result.get(EventTag.TABLE_NAME);
            Object after = result.get(EventTag.AFTER_DATA);
            Object before = result.get(EventTag.BEFORE_DATA);
            Object time = result.get(EventTag.REFERENCE_TIME);
            if (Objects.isNull(eventType) &&
                    Objects.isNull(tableNameObj) &&
                    Objects.isNull(after) &&
                    Objects.isNull(before) &&
                    Objects.isNull(time)) {
                data.put(EventTag.AFTER_DATA, dataMap);
                data.put(EventTag.EVENT_TYPE, insert);
                data.put(EventTag.TABLE_NAME, tableName);
                data.put(EventTag.REFERENCE_TIME, System.currentTimeMillis());
            } else {
                if (Objects.isNull(tableNameObj)) {
                    result.put(EventTag.TABLE_NAME, tableName);
                }
                data = result;
            }
        } else {
            data.put(EventTag.AFTER_DATA, dataMap);
            data.put(EventTag.EVENT_TYPE, insert);
            data.put(EventTag.TABLE_NAME, tableName);
            data.put(EventTag.REFERENCE_TIME, System.currentTimeMillis());
        }
        return data;
    }

    public static boolean eventType(String eventType) {
        return insert.equals(eventType) || update.equals(eventType) || delete.equals(eventType);
    }
}
