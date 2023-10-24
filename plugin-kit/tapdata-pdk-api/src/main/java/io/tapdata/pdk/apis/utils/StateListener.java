package io.tapdata.pdk.apis.utils;

public interface StateListener<T> {
    void stateChanged(T from, T to);
}
