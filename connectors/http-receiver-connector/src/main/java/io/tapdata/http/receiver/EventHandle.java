package io.tapdata.http.receiver;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.http.util.Tags;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author GavinXiao
 * @description EventHandle create by Gavin
 * @create 2023/5/17 19:03
 **/
public class EventHandle {
    public static final String TAG = EventHandle.class.getSimpleName();

    public static TapEvent event(String tableName, Map<String,Object> data) {
        if (null == data || data.isEmpty()){
            //throw new CoreException("The event map is empty, please ensure that your script has an actual return value.");
            TapLogger.info(TAG, "The event map is empty, please ensure that your script has an actual return value.");
            return null;
        }
        Object opTypeObj = data.get(Tags.OP_TYPE_KEY);
        String eventType = Tags.getOp(opTypeObj);
        TapEvent event;
        Long time = (Long) Optional.ofNullable(data.get(Tags.EVENT_REFERENCE_TIME)).orElse(System.currentTimeMillis());
        Object oAfter = data.get(Tags.EVENT_AFTER_KAY);
        Map<String, Object> eventData = oAfter instanceof Map ? (Map<String, Object>) oAfter : data;
        Object oBefore = data.get(Tags.EVENT_BEFORE_KAY);
        Map<String, Object> before = oAfter instanceof Map && oBefore instanceof Map ? (Map<String, Object>) oBefore : null;
        //if (null == eventData){
        //    throw new CoreException("Can not find event data in data event map, please ensure or add an event data by use key named 'data' to as data event in event body map.");
        //}
        switch (eventType){
            case Tags.OP_DELETE:
                before = oBefore instanceof Map ? (Map<String, Object>) oBefore : null;
                event = TapSimplify.deleteDMLEvent(before, tableName).referenceTime(time);
                break;
            case Tags.OP_UPDATE:
                event = TapSimplify.updateDMLEvent(before, eventData, tableName).referenceTime(time);
                break;
            default:
                event = TapSimplify.insertRecordEvent(eventData, tableName).referenceTime(time);
        }
        return event;
    }

    public static List<TapEvent> eventList(String tableName, Object eventData){
        List<TapEvent> events = new ArrayList<>();
        if (null == eventData) return events;
        if (eventData instanceof Map){
            eventList(events, tableName, (Map<String, Object>) eventData);
        }else if (eventData instanceof Collection){
            eventList(events, tableName, (Collection<?>)eventData);
        }else if (eventData.getClass().isArray()){
            eventList(events, tableName, (Object[])eventData);
        }else {
            TapLogger.info(TAG, "Unrecognized event structure, please use a List or Map to represent event data.");
        }
        return events;
    }

    private static void eventList(List<TapEvent> events, String tableName, Map<String, Object> map){
        if (null == map || map.isEmpty()) return;
        TapEvent tapEvent = event(tableName, map);
        if (null != tapEvent) {
            events.add(tapEvent);
        }
    }

    private static void eventList(List<TapEvent> events, String tableName, Collection<?> collection){
        if (null == collection || collection.isEmpty()) return;
        for (Object oMap : collection) {
            if (null == oMap) continue;
            List<TapEvent> list = eventList(tableName, oMap);
            if (list.isEmpty()) continue;
            events.addAll(list);
        }
    }

    private static void eventList(List<TapEvent> events, String tableName, Object[] array){
        if (null == array || array.length == 0 ) return;
        for (Object oArray : array) {
            if (null == oArray) continue;
            List<TapEvent> list = eventList(tableName, oArray);
            if (list.isEmpty()) continue;
            events.addAll(list);
        }
    }
}
