package io.tapdata.entity.codec.filter;

public interface Replacer<T> {
    T replace(T t, boolean needClone);
}
