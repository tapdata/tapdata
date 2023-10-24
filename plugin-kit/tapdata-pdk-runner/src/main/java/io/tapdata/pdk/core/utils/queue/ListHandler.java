package io.tapdata.pdk.core.utils.queue;

import java.util.List;

public interface ListHandler<T>{
    void execute(List<T> list) throws Throwable;
}
