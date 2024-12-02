package com.tapdata.processor.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author harvey
 * @Description
 * @create 2023-03-16 19:20
 **/
@TapExClass(code = 24, module = "Row Filter Processor", prefix = "RFN", describe = "")
public interface RowFilterProcessorExCode_24 {
	@TapExCode
	String UNKNOWN_ERROR = "24001";
	@TapExCode(
			describe = "Row Filter node failed to execute JavaScript code",
			solution = "Check if there are any syntax errors in the filtering code according to the error prompt. If there are, please fix them and restart the task",
			dynamicDescription = "Data: {}",
			describeCN = "Row Filter节点执行JavaScript代码失败",
			solutionCN = "根据报错提示检查过滤代码是否存在语法错误，若存在请修复后重新启动任务",
			dynamicDescriptionCN = "数据：{}"
	)
	String JAVA_SCRIPT_ERROR = "24002";

}
