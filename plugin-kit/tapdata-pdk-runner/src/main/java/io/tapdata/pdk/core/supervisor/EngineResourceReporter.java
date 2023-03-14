package io.tapdata.pdk.core.supervisor;

import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;

public class EngineResourceReporter implements MemoryFetcher {

    public DataMap fetch(String level) {
        return null;
    }

    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        return null;
    }
}