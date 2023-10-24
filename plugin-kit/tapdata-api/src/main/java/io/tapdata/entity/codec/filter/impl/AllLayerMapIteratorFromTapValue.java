package io.tapdata.entity.codec.filter.impl;

import io.tapdata.entity.codec.filter.EntryFilter;
import io.tapdata.entity.codec.filter.MapIteratorEx;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapMapValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * {
 *     "a" : "1",
 *     "b" : {
 *         "c" : 1,
 *         "d" : {
 *             "e" : 1
 *         }
 *     }
 * }
 * keys are below
 * a
 * b
 * b.c
 * b.d
 * b.d.e
 *
 */
public class AllLayerMapIteratorFromTapValue implements MapIteratorEx {
//    @Override
//    public void iterate(Map<String, Object> map, Consumer<Map.Entry<String, Object>> consumer) {
//        Set<Map.Entry<String, Object>> entrySet = map.entrySet();
//        for(Map.Entry<String, Object> entry : entrySet) {
//            if(entry.getValue() instanceof Map) {
//                iterateWithPrefix(entry.getKey() + ".", (Map<String, Object>) entry.getValue(), consumer);
//            } else if(entry.getValue() instanceof Collection) {
//                iterateListWithPrefix(entry.getKey() + ".#", (Collection<Object>) entry.getValue(), consumer);
//            }
//            consumer.accept(entry);
//        }
//    }

    private void iterateWithPrefix(String prefix, Map<String, Object> obj, EntryFilter filter) {
        for (Map.Entry<String, Object> entry : obj.entrySet()) {
            Object value = entry.getValue();
            if(value instanceof TapMapValue) {
                Object newValue = filter.filter(prefix + entry.getKey(), value, true);
                if(newValue != null) {
                    if(newValue instanceof Map) {
                        iterateWithPrefix(prefix + entry.getKey() + MAP_KEY_SEPARATOR, (Map<String, Object>) newValue, filter);
                        entry.setValue(newValue);
                    } else {
                        entry.setValue(newValue);
                    }
                }
            } else if(entry.getValue() instanceof TapArrayValue) {
//                iterateListWithPrefix(prefix + entry.getKey() + ".#", (Collection<Object>) entry.getValue(), newList, filter);
                Object newValue = filter.filter(prefix + entry.getKey(), value, true);
                if(newValue != null) {
                    if(newValue instanceof Collection) {
                        Collection<Object> newList = new ArrayList<>();
                        iterateListWithPrefix(prefix + entry.getKey() + ARRAY_KEY_SEPARATOR, (Collection<Object>) newValue, newList, filter);
                        entry.setValue(newList);
                    } else {
                        entry.setValue(newValue);
                    }
                }
            } else {
                Object newValue = filter.filter(prefix + entry.getKey(), value, true);
                if(newValue != null) {
                    entry.setValue(newValue);
                }
            }
        }
    }

    private void iterateListWithPrefix(String prefix, Collection<Object> collection, Collection<Object> newList, EntryFilter filter) {
        int i = 0;
        for (Object entry : collection) {
            Object value = entry;
            if(value instanceof TapMapValue) {
                Object newValue = filter.filter(prefix + i, value, true);
                if(newValue != null) {
                    if(newValue instanceof Map) {
                        newList.add(newValue);
                        iterateWithPrefix(prefix + i + MAP_KEY_SEPARATOR, (Map<String, Object>) newValue, filter);
                    } else {
                        newList.add(newValue);
                    }
                }
            } else if(value instanceof TapArrayValue) {
//                iterateListWithPrefix(prefix + i + ".#", (Collection<Object>) entry, newList, filter);
                Object newValue = filter.filter(prefix + i, value, true);
                if(newValue != null) {
                    if(newValue instanceof Collection) {
                        Collection<Object> newList1 = new ArrayList<>();
                        iterateListWithPrefix(prefix + i + ARRAY_KEY_SEPARATOR, (Collection<Object>) newValue, newList1, filter);
                        newList.add(newList1);
                    } else {
                        newList.add(newValue);
                    }
                }
            } else {
                Object newValue = filter.filter(prefix + i, value, true);
                if(newValue != null) {
                    newList.add(newValue);
                } else {
                    newList.add(value);
                }
            }
            i++;
        }
    }

    @Override
    public void iterate(Map<String, Object> map, EntryFilter filter) {
        if(map == null || filter == null) {
            return;
        }
        Set<Map.Entry<String, Object>> entrySet = map.entrySet();
        for(Map.Entry<String, Object> entry : entrySet) {
            Object value = entry.getValue();
            if(value instanceof TapMapValue) {
                Object newValue = filter.filter(entry.getKey(), value, false);
                if(newValue != null) {
                    if(newValue instanceof Map) {
                        entry.setValue(newValue);
                        iterateWithPrefix(entry.getKey() + MAP_KEY_SEPARATOR, (Map<String, Object>) newValue, filter);
                    } else {
                        entry.setValue(newValue);
                    }
                }
            } else if(value instanceof TapArrayValue) {
                Object newValue = filter.filter(entry.getKey(), value, false);
                if(newValue != null) {
                    if(newValue instanceof Collection) {
                        Collection<Object> newList = new ArrayList<>();
                        iterateListWithPrefix(entry.getKey() + ARRAY_KEY_SEPARATOR, (Collection<Object>) newValue, newList, filter);
                        entry.setValue(newList);
                    } else {
                        entry.setValue(newValue);
                    }
                }
            } else {
                Object newValue = filter.filter(entry.getKey(), value, false);
                if(newValue != null) {
                    entry.setValue(newValue);
                }
            }
        }
    }

}
