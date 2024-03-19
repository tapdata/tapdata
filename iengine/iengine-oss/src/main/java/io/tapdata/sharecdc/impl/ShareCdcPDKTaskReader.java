package io.tapdata.sharecdc.impl;

import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;

import java.io.Serializable;

public class ShareCdcPDKTaskReader extends ShareCdcHZReader implements Serializable, MemoryFetcher {
    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        return null;
    }
}
