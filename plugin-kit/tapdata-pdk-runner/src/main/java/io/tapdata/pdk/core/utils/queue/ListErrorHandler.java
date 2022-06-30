package io.tapdata.pdk.core.utils.queue;

import java.util.List;

public interface ListErrorHandler<T> {
    void error(List<T> list, Throwable throwable);
}
