package io.tapdata.pdk.core.utils.queue;

public interface Handler<T>{
    void execute(T t) throws Throwable;
}
