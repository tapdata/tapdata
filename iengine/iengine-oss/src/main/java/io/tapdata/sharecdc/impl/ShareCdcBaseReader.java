package io.tapdata.sharecdc.impl;

import io.tapdata.error.EngineErrorCodes;
import io.tapdata.error.EngineExCode_33;
import io.tapdata.exception.TapOssNonsupportFunctionException;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcContext;
import io.tapdata.flow.engine.V2.sharecdc.ShareCdcReader;
import io.tapdata.flow.engine.V2.sharecdc.exception.ShareCdcUnsupportedException;

import java.io.IOException;

public class ShareCdcBaseReader implements ShareCdcReader {
    protected ShareCdcBaseReader() {
        throw new TapOssNonsupportFunctionException();
    }
    @Override
    public void init(ShareCdcContext shareCdcContext) throws ShareCdcUnsupportedException {
        throw new TapOssNonsupportFunctionException();
    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public void close() throws IOException {
        throw new TapOssNonsupportFunctionException();
    }
}
