package io.tapdata.entity.utils.cache;


public interface KVReadOnlyMap<T> {
    T get(String key);
}
