package io.tapdata.pdk.core.utils.queue;

public interface ErrorHandler<T> {
    void error(T t, Throwable throwable);
}
