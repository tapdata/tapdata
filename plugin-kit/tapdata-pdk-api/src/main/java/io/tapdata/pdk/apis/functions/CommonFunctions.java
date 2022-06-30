package io.tapdata.pdk.apis.functions;

import io.tapdata.pdk.apis.functions.common.MemoryFetcherFunction;
import io.tapdata.pdk.apis.functions.connector.target.QueryIndexesFunction;

public class CommonFunctions<T extends CommonFunctions<?>> implements Functions {
    private MemoryFetcherFunction memoryFetcherFunction;
    public T supportMemoryFetcher(MemoryFetcherFunction function) {
        memoryFetcherFunction = function;
        return (T) this;
    }

    public MemoryFetcherFunction getMemoryFetcherFunction() {
        return memoryFetcherFunction;
    }
}
