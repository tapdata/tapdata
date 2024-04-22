package io.tapdata.flow.engine.V2.node.hazelcast.processor.unwind;

import com.tapdata.constant.MapUtil;
import com.tapdata.tm.commons.dag.process.UnwindProcessNode;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import org.apache.commons.lang3.StringUtils;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author GavinXiao
 * @description UnWindNodeUtil create by Gavin
 * @create 2023/10/8 18:01
 * @doc https://www.mongodb.com/docs/manual/reference/operator/aggregation/unwind/
 **/
public class UnWindNodeUtil {

    public static List<TapEvent> initHandel(UnwindProcessNode node, TapEvent event) {
        List<TapEvent> list = new ArrayList<>();
        if (null == node) {
            list.add(event);
            return list;
        }
        final String path = node.getPath();
        if (null == path || "".equals(path.trim())) list.add(event);
        return list;
    }

    /**
     * 从event中获取before
     * */
    public static  Map<String, Object> getBefore(TapEvent event) {
        if (event instanceof TapDeleteRecordEvent) {
            return ((TapDeleteRecordEvent)event).getBefore();
        }
        if (event instanceof TapUpdateRecordEvent) {
            return ((TapUpdateRecordEvent)event).getBefore();
        }
        return null;
    }

    /**
     * 从event中获取after
     * */
    public static  Map<String, Object> getAfter(TapEvent event) {
        if (event instanceof TapInsertRecordEvent) {
            return ((TapInsertRecordEvent)event).getAfter();
        }
        if (event instanceof TapUpdateRecordEvent) {
            return ((TapUpdateRecordEvent)event).getAfter();
        }
        return null;
    }

    /**
     * 判断record中是否存在path路径的key，存在时标记containsKey为true，并返回对应的上一级map对象
     * */
    public static  Map<String, Object> containsParentFromPath(String path, Map<String, Object> record, final AtomicBoolean containsKey) {
        if (record.containsKey(path)) {
            containsKey.set(true);
            return record;
        }
        Map<String, Object> result = record;
        String[] keys = path.split("\\.");
        for (int index = 0; index < keys.length - 1; index++) {
            Object fromMap = getFromMap(result, keys[index], containsKey);
            if (!(fromMap instanceof Map)) {
                return null;
            }
            result = (Map<String, Object>) fromMap;
        }
        return result;
    }

    /**
     * 判断record中是否存在path路径的key，存在时标记containsKey为true，并返回对应的值对象
     * */
    public static  Object containsPath(String path, Map<String, Object> record, final AtomicBoolean containsKey) {
        if (record.containsKey(path)) {
            containsKey.set(true);
            return record.get(path);
        }
        Object result = record;
        String[] keys = path.split("\\.");
        for (String key : keys) {
            result = getFromMap((Map<String, Object>) result, key, containsKey);
            if (!(result instanceof Map)) {
                break;
            }
        }
        return result;
    }

    /**
     * record 中存在path路径的kv键值对时，更具unwind节点配置来做对应的操作
     * */
    public static  Map<String, Object> containsPathAndSetValue(String path, Map<String, Object> map, Object value, String includeArrayIndex, long arrayIndexValue,Boolean flatten,String joiner){
        Map<String, Object> copyMap = new HashMap<>();
        MapUtil.copyToNewMap(map, copyMap);
        serializationFlattenFields(path,copyMap,value,flatten,joiner);
        if (copyMap.containsKey(path)) {
            copyMap.put(path, value);
            containsPathAndSetValue(copyMap, includeArrayIndex, arrayIndexValue);
            return copyMap;
        }
        Map<String, Object> result = copyMap;
        if(!flatten){
            String[] keys = path.split("\\.");
            final AtomicBoolean containsKey = new AtomicBoolean(false);
            for (int index = 0; index < keys.length - 1; index++) {
                Object fromMap = getFromMap(result, keys[index], containsKey);
                if (!(fromMap instanceof Map)) {
                    return null;
                }
                result = (Map<String, Object>) fromMap;
            }
            result.put(keys[keys.length - 1], value);
        }
        containsPathAndSetValue(result, includeArrayIndex, arrayIndexValue);
        return copyMap;
    }

    /**
     * includeArrayIndex 存在时， 在map中添加索引字段
     * */
    public static  void containsPathAndSetValue(Map<String, Object> record, String includeArrayIndex, Long arrayIndexValue) {
        if (null != includeArrayIndex && !"".equals(includeArrayIndex.trim())) {
            record.put(includeArrayIndex, arrayIndexValue);
        }
    }

    /**
     * 从map中根据key获取值， key存在时containsKey标记为true
     * */
    public static  Object getFromMap(Map<String, Object> map, String key, final AtomicBoolean containsKey) {
        if (map.containsKey(key)) {
            containsKey.set(true);
            return map.get(key);
        } else {
            containsKey.set(false);
        }
        return null;
    }

    /**
     * 处理List类型
     * */
    public static  boolean collection(Collection<?> result,
                               List<TapEvent> events,
                               String includeArrayIndex,
                               boolean preserveNullAndEmptyArrays,
                               Map<String, Object> map,
                               String path,
                               Map<String, Object> parentMap,
                               String[] split,
                               TapEvent event,
                               EventHandel handel,
                                      Boolean flatten,
                                      String joiner) {
        if (result.isEmpty()) {
            if (preserveNullAndEmptyArrays) {
                if (map.containsKey(path)) {
                    parentMap.remove(path);
                } else {
                    parentMap.remove(split[split.length - 1]);
                }
                events.add(event);
            } else {
                addEvent(event, events);
            }
            return true;
        }
        int index = 0;
        for (Object item : result) {
            handel.copyEvent(events, containsPathAndSetValue(path, map, item, includeArrayIndex, index,flatten,joiner), event);
            index++;
        }
        return false;
    }

    /**
     * 处理数组类型
     * */
    public static  boolean array(Object[] arr,
                          List<TapEvent> events,
                          String includeArrayIndex,
                          boolean preserveNullAndEmptyArrays,
                          Map<String, Object> map,
                          String path,
                          Map<String, Object> parentMap,
                          String[] split,
                          TapEvent event,
                          EventHandel handel,
                                 Boolean flatten,
                                 String joiner
                                 ) {
        if (arr.length < 1) {
            if (preserveNullAndEmptyArrays) {
                if (map.containsKey(path)) {
                    parentMap.remove(path);
                } else {
                    parentMap.remove(split[split.length - 1]);
                }
                events.add(event);
            } else {
                addEvent(event, events);
            }
            return true;
        }
        for (int index = 0; index < arr.length; index++) {
            handel.copyEvent(events, containsPathAndSetValue(path, map, arr[index], includeArrayIndex, index,flatten,joiner), event);
        }
        return false;
    }

    /**
     * 处理普通对象类型
     * */
    public static  boolean object(Object result,
                           AtomicBoolean containsKey,
                           List<TapEvent> events,
                           String includeArrayIndex,
                           boolean preserveNullAndEmptyArrays,
                           Map<String, Object> parentMap,
                           TapEvent event,
                           EventHandel handel) {
        if (containsKey.get() && null == result && !preserveNullAndEmptyArrays) {
            addEvent(event,events);
            return true;
        }
        if (containsKey.get()) {
            containsPathAndSetValue(parentMap, includeArrayIndex, null);
        }
        events.add(event);
        return false;
    }

    /**
     * 统一处理方式
     * */
    public static  List<TapEvent> handelList(UnwindProcessNode node, TapEvent event, Map<String, Object> map, EventHandel handel) {
        List<TapEvent> events = new ArrayList<>();
        final String path = node.getPath();
        final String includeArrayIndex = node.getIncludeArrayIndex();
        final boolean preserveNullAndEmptyArrays = node.isPreserveNullAndEmptyArrays();
        final AtomicBoolean containsKey = new AtomicBoolean(false);
        if (null != map) {
            String[] split = path.split("\\.");
            Map<String, Object> parentMap = UnWindNodeUtil.containsParentFromPath(path, map, containsKey);
            if (null == parentMap || !containsKey.get()) {
                if (preserveNullAndEmptyArrays) {
                    events.add(event);
                } else {
                    addEvent(event, events);
                }
                return events;
            }
            Object result = containsPath(path, map, containsKey);
            if (result instanceof Collection) {
                if (collection((Collection<?>) result, events, includeArrayIndex, preserveNullAndEmptyArrays, map, path, parentMap, split, event, handel,node.whetherFlatten(),node.getJoiner())) {
                    return events;
                }
            } else if (null != result && result.getClass().isArray()) {
                if (array((Object[]) result, events, includeArrayIndex, preserveNullAndEmptyArrays, map, path, parentMap, split, event, handel,node.whetherFlatten(),node.getJoiner())) {
                    return events;
                }
            } else {
                object(result, containsKey, events, includeArrayIndex, preserveNullAndEmptyArrays, parentMap, event, handel);
            }
        }
        return events;
    }

    public static void serializationFlattenFields(String path,Map<String, Object> map, Object value,Boolean flatten,String joiner){
        if (null == value || null == map)return;
        if(value instanceof Map && flatten){
           Map<String,Object> object = (Map<String,Object>) value;
           map.remove(path);
           if(StringUtils.isBlank(joiner))return;
           for(Map.Entry<String, Object> entry :object.entrySet()){
               map.put(path + joiner + entry.getKey(),entry.getValue());
            }
        }else if(flatten){
            map.put(path,value.toString());
        }
    }

    public static TapDeleteRecordEvent toDeleteEvent(TapRecordEvent event) {
        if (event instanceof TapDeleteRecordEvent) {
            return (TapDeleteRecordEvent) event;
        } else if(event instanceof TapUpdateRecordEvent) {
            Map<String, Object> before = ((TapUpdateRecordEvent) event).getBefore();
            if (null != before && !before.isEmpty()) {
                TapDeleteRecordEvent delete = TapDeleteRecordEvent.create().before(before);
                delete.setReferenceTime(event.getReferenceTime());
                delete.table(event.getTableId());
                return delete;
            }
        }
        return null;
    }

    public static void addEvent(TapEvent event, List<TapEvent> events) {
        TapDeleteRecordEvent e = toDeleteEvent((TapRecordEvent) event);
        if (null != e) {
            events.add(e);
        }
    }
}
