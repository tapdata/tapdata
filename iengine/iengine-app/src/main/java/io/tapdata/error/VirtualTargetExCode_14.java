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
			describe = "JavaScript node model declaration failed to execute",
			solution = "Check if there are any syntax errors in the model declaration code. If so, please fix them",
			dynamicDescription = "Node name of failed model declaration execution: {}, node id: {}, model declaration table name: {}",
			describeCN = "JavaScript节点模型声明执行失败",
			solutionCN = "根据报错信息中提示的表名与节点名，检查模型声明代码是否存在语法错误，若存在请修复",
			dynamicDescriptionCN = "模型声明执行失败的节点名称：{}, 节点id：{}, 模型声明表名：{}"
	)
	String DECLARE_ERROR = "14002";

}
