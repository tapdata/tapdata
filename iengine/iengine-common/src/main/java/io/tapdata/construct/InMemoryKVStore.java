package io.tapdata.construct;

import io.tapdata.construct.constructImpl.KVStore;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryKVStore<T> implements KVStore<T> {

    private final Map<String, T> store = new ConcurrentHashMap<>();
    private final String name;

    public InMemoryKVStore(String name) {
        this.name = name;
        this.init(null);
    }

    @Override
    public void init(Map<String, Object> config) {
        // 内存存储无需初始化特殊配置
    }

    @Override
    public void insert(String key, T value) {
        store.put(key, value);
    }

    @Override
    public void update(String key, T value) {
        store.put(key, value);
    }

    @Override
    public void upsert(String key, T value) {
        store.put(key, value);
    }

    @Override
    public void delete(String key) {
        store.remove(key);
    }

    @Override
    public T find(String key) {
        return store.get(key);
    }

    @Override
    public boolean exists(String key) {
        return store.containsKey(key);
    }

    @Override
    public boolean isEmpty() {
        return store.isEmpty();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return "memory";
    }

    @Override
    public Map<String, T> findByPrefix(String prefix) {
        Map<String, T> result = new HashMap<>();
        for (Map.Entry<String, T> entry : store.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    @Override
    public void clear() {
        store.clear();
    }
}