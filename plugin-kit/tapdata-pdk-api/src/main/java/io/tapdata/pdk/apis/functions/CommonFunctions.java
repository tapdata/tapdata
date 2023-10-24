package io.tapdata.pdk.apis.functions;

import io.tapdata.pdk.apis.functions.common.MemoryFetcherFunction;
import io.tapdata.pdk.apis.functions.common.MemoryFetcherFunctionV2;
import io.tapdata.pdk.apis.functions.connector.target.QueryIndexesFunction;

public class CommonFunctions<T extends CommonFunctions<?>> implements Functions {
    protected MemoryFetcherFunction memoryFetcherFunction;
    protected MemoryFetcherFunctionV2 memoryFetcherFunctionV2;
    @Deprecated
    public T supportMemoryFetcher(MemoryFetcherFunction function) {
        memoryFetcherFunction = function;
        return (T) this;
    }

    public T supportMemoryFetcherV2(MemoryFetcherFunctionV2 function) {
        memoryFetcherFunctionV2 = function;
        return (T) this;
    }

    public MemoryFetcherFunction getMemoryFetcherFunction() {
        return memoryFetcherFunction;
    }

    public MemoryFetcherFunctionV2 getMemoryFetcherFunctionV2() {
        return memoryFetcherFunctionV2;
    }
}
