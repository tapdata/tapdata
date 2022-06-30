package io.tapdata.entity.utils;


import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class DefaultConcurrentMap extends ConcurrentHashMap<String, Object> {
    public <T> T getByDefault(String key, T defaultValue) {
        T value = (T) super.get(key);
        if(value == null) {
            return defaultValue;
        }
        return value;
    }
}
