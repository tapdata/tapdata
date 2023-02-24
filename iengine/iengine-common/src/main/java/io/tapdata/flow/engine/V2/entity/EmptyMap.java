package io.tapdata.flow.engine.V2.entity;

import io.tapdata.entity.utils.cache.KVMap;

public class EmptyMap implements KVMap<Object> {
    @Override
    public void init(String s, Class<Object> aClass) {

    }

    @Override
    public void put(String s, Object o) {

    }

    @Override
    public Object putIfAbsent(String s, Object o) {
        return null;
    }

    @Override
    public Object remove(String s) {
        return null;
    }

    @Override
    public void clear() {

    }

    @Override
    public void reset() {

    }

    @Override
    public Object get(String s) {
        return null;
    }
}
