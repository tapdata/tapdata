package com.tapdata.tm.taskinspect.vo;

import java.util.LinkedHashMap;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/25 10:54 Create
 */
public class MapCreator<K, V> extends LinkedHashMap<K, V> {

    public MapCreator<K, V> add(K key, V value) {
        put(key, value);
        return this;
    }

    public static <K, V> MapCreator<K, V> create(K key, V value) {
        return new MapCreator<K, V>().add(key, value);
    }
}
