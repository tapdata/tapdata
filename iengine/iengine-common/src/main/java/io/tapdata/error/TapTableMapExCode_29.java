package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

@TapExClass(code = 29, module = "Tap Table Map", prefix = "TTM")
public interface TapTableMapExCode_29 {
    @TapExCode
    String UNKNOWN_ERROR = "29001";
    @TapExCode(
            describe = "The engine needs to run the task using the model deduced before the management side is started. This error is thrown when the engine cannot retrieve the model from the management interface. Explanation: \n" +
                    "1. There is an exception on the management side and the status is unavailable. Cause the engine to access the management side interface failure",
            describeCN = "引擎运行任务时需要使用管理端启动前推演出来的模型。当引擎访问管理端接口获取不到模型时，则会报此错误。原因分析：\n" +
                    "1、在启动任务之前，存在某个表模型推演失败，导致获取不到表模型",
            dynamicDescription = "Table {} find schema failed",
            dynamicDescriptionCN = "查找表名为{}的表模型失败",
            solution = "Check the running status of the management side. If the running status of the management side is not normal, you need to restart the task after restoring the management side",
            solutionCN = "重置并启动任务，看是否还会出现一样的错误。如果继续出现需要提工单研发排查",
            recoverable = true
    )
    String FIND_SCHEMA_FAILED = "29002";
}
