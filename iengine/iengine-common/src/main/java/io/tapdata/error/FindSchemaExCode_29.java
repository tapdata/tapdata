package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

@TapExClass(code = 29, module = "Find Schema", prefix = "FS")
public interface FindSchemaExCode_29 {
    @TapExCode
    String UNKNOWN_ERROR = "29001";
    @TapExCode(
            describe = "Find schema failed",
            recoverable = true
    )
    String FIND_SCHEMA_FAILED = "29002";
}