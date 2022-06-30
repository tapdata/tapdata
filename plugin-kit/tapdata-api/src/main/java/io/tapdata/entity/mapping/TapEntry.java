package io.tapdata.entity.mapping;

import java.util.Map;


public class TapEntry<K, V> implements Map.Entry<K, V> {
    private K k;
    private V v;
    public TapEntry(K k, V v) {
        this.k = k;
        this.v = v;
    }
    @Override
    public K getKey() {
        return k;
    }

    @Override
    public V getValue() {
        return v;
    }

    @Override
    public V setValue(V value) {
        v = value;
        return v;
    }
}
