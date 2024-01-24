package io.tapdata.observable.metric.py.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

@TapExClass(code = 31, module = "Python Script Processor", prefix = "PSP", describe = "")
public interface PythonScriptProcessorExCode_31 {
    @TapExCode
    String PYTHON_SCRIPT_ZIP_FAILED = "31001";
}
