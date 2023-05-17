package io.tapdata.http.receiver;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.http.util.Tags;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author GavinXiao
 * @description EventHandle create by Gavin
 * @create 2023/5/17 19:03
 **/
public class EventHandle {
    //public String eventType;
    //public Map<String, Object> data;

    //public static EventHandle handle(String type, Map<String, Object> data){
    //    return new EventHandle().type(type).data(data);
    //}

    //public EventHandle type(String eventType){
    //    this.eventType = eventType;
    //    return this;
    //}

    //public EventHandle data(Map<String, Object> data){
    //    this.data = data;
    //    return this;
    //}

    public static TapEvent event(String tableName, Object type, Object data){

        return null;
    }

    public static TapEvent event(String tableName, Object eventData){

        return null;
    }

    public static List<TapEvent> eventList(String tableName, Object eventData){
        List<TapEvent> events = new ArrayList<>();
        if (eventData instanceof Map){
            Map<String, Object> map = (Map<String, Object>) eventData;
            Object opType = map.get(Tags.OP_TYPE_KEY);
            if (Tags.isOp(opType)){

            }else {

            }
            events.add(EventHandle.event(tableName, map));
        }else if (eventData instanceof Collection){

        }else if (eventData.getClass().isArray()){

        }else {

        }
        return events;
    }


}
