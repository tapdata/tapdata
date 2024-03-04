package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

@TapExClass(code = 29, module = "Tap Table Map", prefix = "TTM")
public interface TapTableMapExCode_29 {
    @TapExCode
    String UNKNOWN_ERROR = "29001";
    @TapExCode(
            describe = "Find schema failed",
            recoverable = true
    )
    String FIND_SCHEMA_FAILED = "29002";
    @TapExCode
    String SCHEMA_NOT_EXISTS = "29003";
}