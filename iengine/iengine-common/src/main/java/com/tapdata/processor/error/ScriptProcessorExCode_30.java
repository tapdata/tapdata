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
            describe = "A syntax error in the Python script in the Python node of the task prevented the creation of the Python execution engine",
            describeCN = "任务中的Python节点中的Python脚本中存在语法错误，导致无法创建Python执行引擎",
            dynamicDescription = "A syntax error occurred in the Python script with the following error message :{}",
            dynamicDescriptionCN = "Python脚本中存在语法错误，错误信息为:{}",
            solution = "According to the error message, check and modify the script error code in the Python node, and restart the task after the trial run passes",
            solutionCN = "根据报错提示，检查并修改Python节点中的脚本错误代码，试运行通过后重新后重新启动任务"
    )
    String GET_PYTHON_ENGINE_FAILED = "30011";

    @TapExCode(
            describe = "This error occurs when a standard JS or enhanced JS node in a task has an error while executing a JS script",
            describeCN = "任务中的标准JS或增强JS节点在执行JS脚本时发生错误时，则会出现此报错",
            dynamicDescription = "There was an error in the JS script with the following message:{}",
            dynamicDescriptionCN = "JS脚本中存在错误，错误信息为:{}",
            solution = "According to the error information, check and modify the code in the standard JS or enhanced JS script that causes the error, and restart the task after the trial run passes",
            solutionCN = "根据报错信息，检查和修改标准JS或增强JS中脚本中导致错误代码，试运行通过后重新启动任务"
    )
    String JAVA_SCRIPT_PROCESS_FAILED = "30012";
    @TapExCode(
            describe = "This error occurs when a Python node in a task fails while executing a Python script",
            describeCN = "任务中的Python节点在执行Python脚本时发生错误时，则会出现此报错",
            dynamicDescription = "An exception occurred while executing a Python script with the following error message:{}",
            dynamicDescriptionCN = "执行Python脚本时发生异常，错误信息为:{}",
            solution = "Based on the error message, check the code in the Python script that caused the error, and restart the task after the trial run passes",
            solutionCN = "根据报错信息，检查和Python脚本中的导致错误的代码，并试运行通过后重新启动任务"
    )
    String PYTHON_PROCESS_FAILED="30013";
    String SCRIPT_IO="30014";
    @TapExCode(
            describe = "A syntax error in the JS script in the JS node of the task prevented the creation of the JS script engine",
            describeCN = "任务中的标准JS/增强JS节点中的JS脚本中存在语法错误，导致无法创建JS执行引擎",
            dynamicDescription = "A syntax error occurred in the Python script with the following error message :{}",
            dynamicDescriptionCN = "JS脚本中存在语法错误，错误信息为:{}",
            solution = "According to the error message, check and modify the script error code in the JS node, and restart the task after the trial run passes",
            solutionCN = "根据报错提示，检查并修改JS节点中的脚本错误代码，试运行通过后重新后重新启动任务"
    )
    String JAVA_SCRIPT_PROCESSOR_GET_SCRIPT_FAILED="30015";
    @TapExCode(
            describe = "A syntax error in the js script in the custom js node of the task prevented the creation of the script engine",
            describeCN = "任务中的自定义节点中的JS脚本中存在语法错误，导致无法创建JS执行引擎",
            dynamicDescription = "A syntax error occurred in the js script with the following error message :{}",
            dynamicDescriptionCN = "自定义JS脚本中存在语法错误，错误信息为:{}",
            solution = "According to the error message, check and modify the script error code in the custom js node, and restart the task after the trial run passes",
            solutionCN = "根据报错提示，检查并修改自定义JS节点中的脚本错误代码，试运行通过后重新后重新启动任务"
    )
    String CUSTOM_PROCESSOR_GET_SCRIPT_ENGINE_FAILED="30016";
}
