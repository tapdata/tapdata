package com.tapdata.tm.async;

import java.io.IOException;

public interface AsyncJobWaiter<T> {
    void jobAccomplished(T result, Throwable error) throws IOException;
}
