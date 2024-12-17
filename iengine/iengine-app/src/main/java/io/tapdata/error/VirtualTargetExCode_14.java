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
			describe = "Failed to parse the model",
			solution = "According to the table name and node name prompted in the error message, refer to the usage help of the model declaration to check if there are any syntax errors in the declaration code. If there are, please fix them",
			dynamicDescription = "Node name of failed model declaration execution: {}, node id: {}, model declaration table name: {}",
			describeCN = "解析模型失败",
			solutionCN = "根据报错信息中提示的表名与节点名，参考模型声明的使用帮助，检查声明代码是否存在语法错误，若存在请修复",
			dynamicDescriptionCN = "模型声明执行失败的节点名称：{}, 节点id：{}, 模型声明表名：{}"
	)
	String DECLARE_ERROR = "14002";

}
