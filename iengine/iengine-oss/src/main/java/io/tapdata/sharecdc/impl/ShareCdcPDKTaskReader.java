package io.tapdata.sharecdc.impl;

import io.tapdata.entity.memory.MemoryFetcher;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.exception.TapOssNonsupportFunctionException;

import java.io.Serializable;

public class ShareCdcPDKTaskReader extends ShareCdcHZReader implements Serializable, MemoryFetcher {
    ShareCdcPDKTaskReader(Object offset) {
        throw new TapOssNonsupportFunctionException();
    }
    @Override
    public DataMap memory(String keyRegex, String memoryLevel) {
        return null;
    }
}
