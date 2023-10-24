package io.tapdata.entity.utils.cache;


public interface KVReadOnlyMap<T> {
    T get(String key);
    default Iterator<Entry<T>> iterator() {
        throw new UnsupportedOperationException();
    }
}
