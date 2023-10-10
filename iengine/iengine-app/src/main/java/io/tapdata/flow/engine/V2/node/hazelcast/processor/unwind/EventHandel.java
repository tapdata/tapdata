package io.tapdata.flow.engine.V2.node.hazelcast.processor.unwind;

import com.tapdata.constant.MapUtil;
import com.tapdata.tm.commons.dag.process.UnwindProcessNode;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public interface EventHandel {
    Map<String, EventHandel> handelMap = new ConcurrentHashMap<>();

    public static List<TapEvent> getHandelResult(UnwindProcessNode node, TapEvent event) {
        final String op = event instanceof TapUpdateRecordEvent ? "u" :
                    event instanceof TapInsertRecordEvent ? "i" :
                    event instanceof TapDeleteRecordEvent ? "d" : null;
        if (null == op) return null;
        EventHandel eventHandel = handelMap.get(op);
        if (null == eventHandel) {
            switch (op) {
                case "u" : eventHandel = new UpdateHandel();break;
                case "i" : eventHandel = new InsertHandel();break;
                default: eventHandel = new DeleteHandel();
            }
            handelMap.put(op, eventHandel);
        }


        List<TapEvent> events = eventHandel.defaultHandel(node, event);
        if (!events.isEmpty()) return events;
        return eventHandel.handel(node, event);
    }

    public static void close() {
        handelMap.clear();
    }

    List<TapEvent> handel(UnwindProcessNode node, TapEvent event);

    default List<TapEvent> defaultHandel(UnwindProcessNode node, TapEvent event) {
        List<TapEvent> list = new ArrayList<>();
        if (null == node) list.add(event);
        final String path = node.getPath();
        if (null == path || "".equals(path.trim())) list.add(event);
        return list;
    }

    default Map<String, Object> getBefore(TapEvent event) {
        if (event instanceof TapDeleteRecordEvent) {
            return ((TapDeleteRecordEvent)event).getBefore();
        }
        if (event instanceof TapUpdateRecordEvent) {
            return ((TapUpdateRecordEvent)event).getBefore();
        }
        return null;
    }

    default Map<String, Object> getAfter(TapEvent event) {
        if (event instanceof TapInsertRecordEvent) {
            return ((TapInsertRecordEvent)event).getAfter();
        }
        if (event instanceof TapUpdateRecordEvent) {
            return ((TapUpdateRecordEvent)event).getAfter();
        }
        return null;
    }

    static Map<String, Object> containsParentFromPath(String path, Map<String, Object> record, final AtomicBoolean containsKey) {
        if (record.containsKey(path)) {
            containsKey.set(true);
            return record;
        }
        Map<String, Object> result = record;
        String[] keys = path.split("\\.");
        for (int index = 0; index < keys.length - 1; index++) {
            Object fromMap = getFromMap((Map<String, Object>) result, keys[index], containsKey);
            if (!(fromMap instanceof Map)) {
                return null;
            }
            result = (Map<String, Object>) fromMap;
        }
        return result;
    }

    default Object containsPath(String path, Map<String, Object> record, final AtomicBoolean containsKey) {
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

    default Map<String, Object> containsPathAndSetValue(String path, Map<String, Object> record, Object value, String includeArrayIndex, long arrayIndexValue){
        Map<String, Object> copyMap = new HashMap<>();
        MapUtil.copyToNewMap(record, copyMap);
        if (copyMap.containsKey(path)) {
            copyMap.put(path, value);
            containsPathAndSetValue(copyMap, includeArrayIndex, arrayIndexValue);
            return copyMap;
        }
        Map<String, Object> result = copyMap;
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
        containsPathAndSetValue(result, includeArrayIndex, arrayIndexValue);
        return copyMap;
    }

    default void containsPathAndSetValue(Map<String, Object> record, String includeArrayIndex, Long arrayIndexValue) {
        if (null != includeArrayIndex && !"".equals(includeArrayIndex.trim())) {
            record.put(includeArrayIndex, arrayIndexValue);
        }
    }

    static Object getFromMap(Map<String, Object> map, String key, final AtomicBoolean containsKey) {
        if (map.containsKey(key)) {
            containsKey.set(true);
            return map.get(key);
        } else {
            containsKey.set(false);
        }
        return null;
    }
}

class InsertHandel implements EventHandel {
    @Override
    public List<TapEvent> handel(UnwindProcessNode node, TapEvent event) {
        List<TapEvent> events = new ArrayList<>();
        Map<String, Object> after = getAfter(event);
        final String path = node.getPath();
        final String includeArrayIndex = node.getIncludeArrayIndex();
        final boolean preserveNullAndEmptyArrays = node.isPreserveNullAndEmptyArrays();
        final AtomicBoolean containsKey = new AtomicBoolean(false);
        if (null != after) {
            Map<String, Object> parentMap = EventHandel.containsParentFromPath(path, after, containsKey);
            if (null == parentMap || !containsKey.get()) {
                if (preserveNullAndEmptyArrays) {
                    events.add(event);
                }
                return events;
            }
            Object result = containsPath(path, after, containsKey);
            if (result instanceof Collection) {
                if (((Collection<?>) result).isEmpty()) {
                    if (preserveNullAndEmptyArrays) {
                        if (after.containsKey(path)) {
                            parentMap.remove(path);
                        } else {
                            String[] split = path.split("\\.");
                            parentMap.remove(split[split.length - 1]);
                        }
                        events.add(event);
                    }
                    return events;
                }
                int index = 0;
                for (Object item : ((Collection<?>) result)) {
                    TapInsertRecordEvent e = TapInsertRecordEvent.create();
                    e.after(containsPathAndSetValue(path, after, item, includeArrayIndex, index));
                    e.setReferenceTime(((TapInsertRecordEvent)event).getReferenceTime());
                    events.add(e);
                    index ++;
                }
            } else if (null != result && result.getClass().isArray()) {
                Object[] arr = (Object[]) result;
                if (arr.length < 1) {
                    if (preserveNullAndEmptyArrays) {
                        if (after.containsKey(path)) {
                            parentMap.remove(path);
                        } else {
                            String[] split = path.split("\\.");
                            parentMap.remove(split[split.length - 1]);
                        }
                        events.add(event);
                    }
                    return events;
                }
                for (int index = 0; index < arr.length; index++) {
                    TapInsertRecordEvent e = TapInsertRecordEvent.create();
                    e.after(containsPathAndSetValue(path, after, arr[index], includeArrayIndex, index));
                    e.setReferenceTime(((TapInsertRecordEvent)event).getReferenceTime());
                    events.add(e);
                }
            } else {
                if (!(null == result && !preserveNullAndEmptyArrays)) {
                    if (containsKey.get()) {
                        containsPathAndSetValue(parentMap, includeArrayIndex, null);
                    }
                    events.add(event);
                }
            }
        }
        return events;
    }
}

class DeleteHandel implements EventHandel {
    @Override
    public List<TapEvent> handel(UnwindProcessNode node, TapEvent event) {
        List<TapEvent> events = new ArrayList<>();
        Map<String, Object> before = getBefore(event);
        final String path = node.getPath();
        final String includeArrayIndex = node.getIncludeArrayIndex();
        final boolean preserveNullAndEmptyArrays = node.isPreserveNullAndEmptyArrays();
        final AtomicBoolean containsKey = new AtomicBoolean(false);
        if (null != before) {
            Map<String, Object> parentMap = EventHandel.containsParentFromPath(path, before, containsKey);
            if (null == parentMap || !containsKey.get()) {
                if (preserveNullAndEmptyArrays) {
                    events.add(event);
                }
                return events;
            }
            Object result = containsPath(path, before, containsKey);
            if (result instanceof Collection) {
                if (((Collection<?>) result).isEmpty()) {
                    if (preserveNullAndEmptyArrays) {
                        if (before.containsKey(path)) {
                            parentMap.remove(path);
                        } else {
                            String[] split = path.split("\\.");
                            parentMap.remove(split[split.length - 1]);
                        }
                        events.add(event);
                    }
                    return events;
                }
                int index = 0;
                for (Object item : ((Collection<?>) result)) {
                    TapDeleteRecordEvent e = TapDeleteRecordEvent.create();
                    e.before(containsPathAndSetValue(path, before, item, includeArrayIndex, index));
                    e.setReferenceTime(((TapDeleteRecordEvent)event).getReferenceTime());
                    events.add(e);
                    index ++;
                }
            } else if (null != result && result.getClass().isArray()) {
                Object[] arr = (Object[]) result;
                if (arr.length < 1) {
                    if (preserveNullAndEmptyArrays) {
                        if (before.containsKey(path)) {
                            parentMap.remove(path);
                        } else {
                            String[] split = path.split("\\.");
                            parentMap.remove(split[split.length - 1]);
                        }
                        events.add(event);
                    }
                    return events;
                }
                for (int index = 0; index < arr.length; index++) {
                    TapDeleteRecordEvent e = TapDeleteRecordEvent.create();
                    e.before(containsPathAndSetValue(path, before, arr[index], includeArrayIndex, index));
                    e.setReferenceTime(((TapDeleteRecordEvent)event).getReferenceTime());
                    events.add(e);
                }
            } else {
                if (!(null == result && !preserveNullAndEmptyArrays)) {
                    if (containsKey.get()) {
                        containsPathAndSetValue(parentMap, includeArrayIndex, null);
                    }
                    events.add(event);
                }
            }
        }
        return events;
    }
}

class UpdateHandel implements EventHandel {

    @Override
    public List<TapEvent> handel(UnwindProcessNode node, TapEvent event){
        List<TapEvent> events = new ArrayList<>();
        Map<String, Object> after = getAfter(event);
        Map<String, Object> before = getBefore(event);
        if (null == before || before.isEmpty()) {
            final String path = node.getPath();
            final String includeArrayIndex = node.getIncludeArrayIndex();
            final boolean preserveNullAndEmptyArrays = node.isPreserveNullAndEmptyArrays();
            final AtomicBoolean containsKey = new AtomicBoolean(false);
            String[] split = path.split("\\.");
            if (null != after) {
                Map<String, Object> parentMap = EventHandel.containsParentFromPath(path, after, containsKey);
                if (null == parentMap || !containsKey.get()) {
                    if (preserveNullAndEmptyArrays) {
                        events.add(event);
                    }
                    return events;
                }
                Object result = containsPath(path, after, containsKey);
                if (result instanceof Collection) {
                    if (((Collection<?>) result).isEmpty()) {
                        if (preserveNullAndEmptyArrays) {
                            if (after.containsKey(path)) {
                                parentMap.remove(path);
                            } else {
                                parentMap.remove(split[split.length - 1]);
                            }
                            events.add(event);
                        }
                        return events;
                    }
                    int index = 0;
                    for (Object item : ((Collection<?>) result)) {
                        TapUpdateRecordEvent e = TapUpdateRecordEvent.create();
                        e.after(containsPathAndSetValue(path, after, item, includeArrayIndex, index));
                        e.setReferenceTime(((TapUpdateRecordEvent) event).getReferenceTime());
                        events.add(e);
                        index++;
                    }
                } else if (null != result && result.getClass().isArray()) {
                    Object[] arr = (Object[]) result;
                    if (arr.length < 1) {
                        if (preserveNullAndEmptyArrays) {
                            if (after.containsKey(path)) {
                                parentMap.remove(path);
                            } else {
                                parentMap.remove(split[split.length - 1]);
                            }
                            events.add(event);
                        }
                        return events;
                    }
                    for (int index = 0; index < arr.length; index++) {
                        TapUpdateRecordEvent e = TapUpdateRecordEvent.create();
                        e.after(containsPathAndSetValue(path, after, arr[index], includeArrayIndex, index));
                        e.setReferenceTime(((TapUpdateRecordEvent) event).getReferenceTime());
                        events.add(e);
                    }
                } else {
                    if (!(null == result && !preserveNullAndEmptyArrays)) {
                        if (containsKey.get()) {
                            containsPathAndSetValue(parentMap, includeArrayIndex, null);
                        }
                        events.add(event);
                    }
                }
            }
        } else {
            Long referenceTime = ((TapUpdateRecordEvent) event).getReferenceTime();
            TapDeleteRecordEvent delete = TapDeleteRecordEvent.create();
            delete.before(before);
            delete.referenceTime(referenceTime);
            List<TapEvent> deletes = EventHandel.getHandelResult(node, delete);
            if (null != deletes) {
                events.addAll(deletes);
            }
            TapInsertRecordEvent insert = TapInsertRecordEvent.create();
            insert.after(after);
            insert.referenceTime(referenceTime);
            List<TapEvent> inserts = EventHandel.getHandelResult(node, insert);
            if (null != inserts) {
                events.addAll(inserts);
            }
        }
        return events;
    }
}