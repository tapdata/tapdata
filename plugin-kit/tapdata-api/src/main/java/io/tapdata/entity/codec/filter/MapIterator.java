package io.tapdata.entity.codec.filter;

import java.util.Map;
import java.util.function.Consumer;

public interface MapIterator {
    void iterate(Map<String, Object> map, Consumer<Map.Entry<String, Object>> consumer);
}
