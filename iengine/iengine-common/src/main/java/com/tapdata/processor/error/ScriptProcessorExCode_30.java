package com.tapdata.processor.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

@TapExClass(code = 30, module = "Script Processor", prefix = "SP", describe = "")
public interface ScriptProcessorExCode_30 {
    @TapExCode
    String GET_SCRIPT_ENGINE_ERROR = "30001";
    @TapExCode(
            describe = "When initializing the Python script engine, the URL classloader failed to load the resources required by the Python engine",
            describeCN = "初始化Python脚本引擎时，URL类加载器加载Python引擎需要的资源失败",
            dynamicDescription = "Load resource path :{}",
            dynamicDescriptionCN = "加载资源路径:{}",
            solution = "1. Obtain the resource file under the resource path by contacting the after-sales service to obtain the data synchronization tool installation package or re-unzipping the existing installation package. Then put the resource file back to the corresponding directory path and restart",
            solutionCN = "1. 通过联系售后获取数据同步工具安装包或重新解压已有的安装包获取资源路径下的资源文件。然后将资源文件放回到对应的目录路径下后重新启动"
    )
    String URL_CLASS_LOADER_ERROR = "30002";
    @TapExCode
    String INIT_PYTHON_METHOD_ERROR = "30003";
    @TapExCode(
            describe = "The execution engine failed to execute the JavaScript script",
            describeCN = "脚本引擎在执行脚本失败，可能造成失败的原因：\n" +
                    "1. 脚本中存在语法错误或调用了未定义的方法\n" +
                    "2. 脚本在执行的过程中出现异常",
            dynamicDescription = "Error message if script execution failed :{}",
            dynamicDescriptionCN = "执行脚本失败的错误信息:{}",
            solution = "The task is restarted after checking the script according to the error message of the failed script execution and correcting it",
            solutionCN = "根据执行脚本失败的错误信息检查脚本并修正后重新启动任务"
    )
    String INVOKE_SCRIPT_FAILED = "30004";

    @TapExCode(
            describe = "The standard JS node failed to load the custom function",
            describeCN = "标准JS节点初始化时，加载函数管理中导入的第三方jar包失败",
            solution = "Recompile the third-party jar and upload it again",
            solutionCN = "将第三方jar包重新编译后重新上传"
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
            describeCN = "在初始化JS脚本引擎时，定义JS脚本引擎的函数失败"
    )
    String EVAL_SOURCE_ERROR = "30008";
    @TapExCode(
            describe = "Init Script Engine failed",
            describeCN = "初始化脚本引擎失败"
    )
    String INIT_SCRIPT_ENGINE_FAILED = "30009";
    @TapExCode(
            describe = "The function to define the script engine using the URL classloader failed",
            describeCN = "使用URL类加载器定义脚本引擎的函数失败"
    )
    String APPLY_CLASS_LOADER_CONTEXT_FAILED = "30010";
    @TapExCode(
            describe = "A syntax error in the script in the Python node prevented the Python script engine from being created",
            describeCN = "Python节点中的脚本中存在语法错误，导致无法创建Python脚本引擎",
            dynamicDescription = "Syntax error in Python script, error message :{}",
            dynamicDescriptionCN = "Python脚本中存在语法错误，错误信息:{}",
            solution = "According to the error message, check and modify the script error code in the Python node, and restart the task after the trial run passes",
            solutionCN = "根据报错提示，检查并修改Python节点中的脚本错误代码，试运行通过后重新后重新启动任务"
    )
    String GET_PYTHON_ENGINE_FAILED = "30011";
    @TapExCode(
            describe = "An error occurred while the JS node was executing the script",
            describeCN = "JS节点在执行脚本时发生错误",
            dynamicDescription = "There was an error in the JS script with the following message:{}",
            dynamicDescriptionCN = "JS脚本中存在错误，错误信息为:{}",
            solution = "According to the error information, check and modify the code that causes the error in the JS node, and restart the task after the trial run passes",
            solutionCN = "根据报错信息，检查和修改JS节点中导致错误代码，试运行通过后重新启动任务"
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
    @TapExCode(
            describe = "There is a syntax error in the script in the JS node, and the JS script engine cannot be created",
            describeCN = "JS节点中脚本中存在语法错误，导致无法创建JS脚本引擎",
            dynamicDescription = "There is a syntax error in the JS script with the following message :{}",
            dynamicDescriptionCN = "JS脚本中存在语法错误，错误信息为:{}",
            solution = "According to the error prompt, check and modify the script error code in the JS node, and restart the task after the trial run passes",
            solutionCN = "根据报错提示，检查并修改JS节点中的脚本错误代码，试运行通过后重新后重新启动任务"
    )
    String JAVA_SCRIPT_PROCESSOR_GET_SCRIPT_FAILED="30014";
    @TapExCode(
            describe = "A syntax error in the js script in the custom js node of the task prevented the creation of the script engine",
            describeCN = "自定义节点中的脚本中存在语法错误，导致无法创建JS脚本引擎",
            dynamicDescription = "There is a syntax error in the custom script. The error message is:",
            dynamicDescriptionCN = "自定义脚本中存在语法错误，错误信息为:{}",
            solution = "According to the error message, check and modify the script error code in the custom js node, and restart the task after the trial run passes",
            solutionCN = "根据报错提示，检查并修改自定义JS节点中的脚本错误代码，试运行通过后重新后重新启动任务"
    )
    String CUSTOM_PROCESSOR_GET_SCRIPT_ENGINE_FAILED="30015";

    @TapExCode(
            describe = "Failed to execute the script, detected that the script execution engine is empty",
            describeCN = "执行脚本时失败，检测出脚本引擎为空"
    )
    String INVOKE_SCRIPT_FAILED_ENGINE_NULL ="30016";

}
