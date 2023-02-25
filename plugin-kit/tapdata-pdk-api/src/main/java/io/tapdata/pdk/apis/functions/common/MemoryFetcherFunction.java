package io.tapdata.pdk.apis.functions.common;

import io.tapdata.pdk.apis.functions.connector.TapFunction;

import java.util.List;

@Deprecated
public interface MemoryFetcherFunction extends TapFunction {
    String MEMORY_LEVEL_SUMMARY = "Summary";
    String MEMORY_LEVEL_IN_DETAIL = "Detail";

    String memory(List<String> mapKeys, String memoryLevel);
}
