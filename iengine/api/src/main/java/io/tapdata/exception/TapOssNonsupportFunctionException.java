package io.tapdata.exception;

import io.tapdata.error.EngineExCode_33;
import io.tapdata.error.TaskProcessorExCode_11;

public class TapOssNonsupportFunctionException extends TapCodeException{
    public TapOssNonsupportFunctionException() {
        super(EngineExCode_33.UNABLE_SUPPORT_FUNCTION_EXCEPTION, "The current version does not support this feature");
    }
}
