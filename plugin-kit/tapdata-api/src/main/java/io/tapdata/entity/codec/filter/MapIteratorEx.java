package io.tapdata.entity.codec.filter;

import java.util.Map;
import java.util.function.BiFunction;

public interface MapIteratorEx {
    String MAP_KEY_SEPARATOR = ".";
    String ARRAY_KEY_SEPARATOR = ".#";
    void iterate(Map<String, Object> map, EntryFilter filter);
}
