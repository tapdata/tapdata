package com.tapdata.tm.async;

public interface AsyncJobWaiter<T> {
    void jobAccomplished(T result, Throwable error);
}
