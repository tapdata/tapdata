package com.tapdata.processor.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

@TapExClass(code = 30, module = "Script Processor", prefix = "SP", describe = "")
public interface ScriptProcessorExCode_30 {
    @TapExCode
    String GET_SCRIPT_ENGINE_ERROR = "30001";
    @TapExCode(
            describe = "Loading class by URL failed",
            describeCN = "通过URL加载类失败"
    )
    String URL_CLASS_LOADER_ERROR = "30002";
    @TapExCode
    String INIT_PYTHON_METHOD_ERROR = "30003";
    @TapExCode(
            describe = "Failed to call engine script",
            describeCN = "调用引擎脚本失败"
    )
    String INVOKE_SCRIPT_FAILED = "30004";
    @TapExCode(
            describe = "Failed to initialize standard engine script build method",
            describeCN = "初始化标准引擎脚本构建方法失败"
    )
    String INIT_STANDARDIZATION_METHOD_FAILED = "30005";
    @TapExCode(
            describe = "Failed to get standard engine script",
            describeCN = "获取标准引擎脚本失败"
    )
    String GET_SCRIPT_STANDARDIZATION_ENGINE_FAILED = "30006";
    @TapExCode(
            describe = "Engine script initialization build method failed",
            describeCN = "引擎初始化构建方法失败"
    )
    String INIT_BUILD_IN_METHOD_FAILED = "30007";
    @TapExCode(
            describe = "Engine eval source failed",
            describeCN = "引擎评估源失败"
    )
    String EVAL_SOURCE_ERROR = "30008";
    @TapExCode(
            describe = "Init Script Engine failed",
            describeCN = "初始化脚本引擎失败"
    )
    String INIT_SCRIPT_ENGINE_FAILED = "30009";
    @TapExCode(
            describe = "Apply classloader context failed",
            describeCN = "应用类加载器上下文失败"
    )
    String APPLY_CLASS_LOADER_CONTEXT_FAILED = "30010";

    @TapExCode(
            describe = "Failed to get Python engine",
            describeCN = "获取Python引擎失败"
    )
    String GET_PYTHON_ENGINE_FAILED = "30011";
}
