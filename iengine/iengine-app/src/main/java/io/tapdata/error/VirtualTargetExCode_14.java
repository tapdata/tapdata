package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 19:20
 **/
@TapExClass(code = 14, module = "Virtual Target", prefix = "VTN", describe = "")
public interface VirtualTargetExCode_14 {
	@TapExCode
	String UNKNOWN_ERROR = "14001";
	@TapExCode(
					describe = "JavaScript node model declaration failed to execute.\n" +
									"Reason\n1. Syntax error in model declaration code",
					solution = "1. Check the model declaration code and fix",
					describeCN = "JavaScript节点模型声明执行失败\n" +
									"原因\n1. 模型声明代码存在语法错误",
					solutionCN = "1. 检查模型声明代码"
	)
	String DECLARE_ERROR = "14002";

}
