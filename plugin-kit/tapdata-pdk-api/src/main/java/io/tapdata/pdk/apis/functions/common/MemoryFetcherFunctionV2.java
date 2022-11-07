package io.tapdata.pdk.apis.functions.common;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.functions.connector.TapFunction;

import java.util.List;

public interface MemoryFetcherFunctionV2 extends TapFunction {
    String MEMORY_LEVEL_SUMMARY = "Summary";
    String MEMORY_LEVEL_IN_DETAIL = "Detail";

    DataMap memory(String keyRegex, String memoryLevel);
}
