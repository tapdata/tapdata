package io.tapdata.entity.codec.filter.impl;

import io.tapdata.entity.codec.filter.EntryFilter;
import io.tapdata.entity.codec.filter.MapIterator;
import io.tapdata.entity.codec.filter.MapIteratorEx;

import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;

public class FirstLayerMapIterator implements MapIteratorEx {

    @Override
    public void iterate(Map<String, Object> map, EntryFilter filter) {
        if(map == null || filter == null) {
            return;
        }
        Set<Map.Entry<String, Object>> entrySet = map.entrySet();
        for(Map.Entry<String, Object> entry : entrySet) {
            Object newValue = filter.filter(entry.getKey(), entry.getValue(), false);
            if(newValue != null) {
                entry.setValue(newValue);
            }
        }
    }
}
