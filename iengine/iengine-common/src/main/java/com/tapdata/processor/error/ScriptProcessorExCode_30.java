package com.tapdata.processor.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

@TapExClass(code = 30, module = "Script Processor", prefix = "SP", describe = "")
public interface ScriptProcessorExCode_30 {
    @TapExCode
    String GET_SCRIPT_ENGINE_ERROR = "30001";
    @TapExCode
    String URL_CLASS_LOADER_ERROR = "30002";
    @TapExCode
    String INIT_PYTHON_METHOD_ERROR = "30003";
    @TapExCode
    String INVOKE_SCRIPT_FAILED = "30004";
    @TapExCode
    String INIT_STANDARDIZATION_METHOD_FAILED = "30005";
    @TapExCode
    String GET_SCRIPT_STANDARDIZATION_ENGINE_FAILED = "30006";
    @TapExCode
    String INIT_BUILD_IN_METHOD_FAILED = "30007";
    @TapExCode
    String EVAL_SOURCE_ERROR = "30008";
    @TapExCode
    String INIT_SCRIPT_ENGINE_FAILED = "30009";
    @TapExCode
    String APPLY_CLASS_LOADER_CONTEXT_FAILED = "30010";
}
