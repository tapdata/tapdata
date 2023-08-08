package io.tapdata.exception;

import io.tapdata.PDKExCode_10;

public class TapPdkRetryableEx extends TapPdkBaseException{

    public TapPdkRetryableEx(String pdkId, Throwable cause) {
        super(PDKExCode_10.RETRYABLE_ERROR, pdkId, cause);
    }

    public TapPdkRetryableEx(String message, String pdkId, Throwable cause) {
        super(PDKExCode_10.RETRYABLE_ERROR, message, pdkId, cause);
    }

    @Override
    public String getMessage() {
        return "PDK retry exception (Server Error Code " + serverErrorCode + "): " + super.getMessage();
    }
}
