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
			describe = "JavaScript code execution failed.\n" +
					"Reason\n1. Syntax error in filter code",
			solution = "1. Check the filter code and fix",
			describeCN = "JavaScript代码执行失败\n" +
					"原因\n1. 过滤代码存在语法错误",
			solutionCN = "1. 检查过滤代码"
	)
	String JAVA_SCRIPT_ERROR = "24002";

}
