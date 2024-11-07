package io.tapdata.observable.metric.py.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

@TapExClass(code = 31, module = "Python Script Processor", prefix = "PSP", describe = "")
public interface PythonScriptProcessorExCode_31 {
    @TapExCode(
            describe = "Python script compression failed",
            describeCN = "Python脚本压缩失败",
            dynamicDescription = "zip error from ZipUtils failed,fileName:{},filePath:{}",
            dynamicDescriptionCN = "使用ZipUtils 压缩文件时出现错误，文件名为：{}，文件路径为：{}"
    )
    String PYTHON_SCRIPT_ZIP_FAILED = "31001";
}
