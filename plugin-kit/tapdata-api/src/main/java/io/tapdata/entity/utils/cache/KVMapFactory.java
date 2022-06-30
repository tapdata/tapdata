package io.tapdata.entity.utils.cache;

public interface KVMapFactory {
    <T> KVMap<T> getCacheMap(String mapKey, Class<T> valueClass);

    <T> KVMap<T> getPersistentMap(String mapKey, Class<T> valueClass);

    <T> KVReadOnlyMap<T> createKVReadOnlyMap(String mapKey);

    boolean reset(String mapKey);
}
