package io.tapdata.entity.mapping;

public interface TapIterator<T> {
    boolean iterate(T t);
}
