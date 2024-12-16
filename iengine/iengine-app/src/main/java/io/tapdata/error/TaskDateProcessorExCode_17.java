package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 19:20
 **/
@TapExClass(code = 17, module = "Task Date Processor", prefix = "TDP", describe = "Task date processor")
public interface TaskDateProcessorExCode_17 {
	@TapExCode
	String UNKNOWN_ERROR = "17001";

	@TapExCode(
			describe = "Time calculation node failed to obtain model",
			describeCN = "时间运算节点获取模型失败",
			dynamicDescription = "According to {} obtaining the model, table name: {}, node name: {}, node ID: {}",
			dynamicDescriptionCN = "根据 ｛｝ 获取模型，表名：｛｝，节点名：｛}，节点ID:｛｝",
			solution = "Choose any of the following ways to solve: \n" +
					"1. Return to editing mode, click save to trigger deduction, and pay attention to whether there are any errors in the task log\n" +
					"2. Based on the node name and node ID prompted in the error message, reload the model and start the task",
			solutionCN = "选择以下任意方式解决：\n" +
					"1. 回到编辑状态点击保存触发推演，关注任务日志中有无报错\n" +
					"2. 根据报错信息中提示的节点名与节点ID，重新加载模型后启动任务"
	)
	String INIT_TARGET_TABLE_TAP_TABLE_NULL = "17002";

	@TapExCode(
			describe = "The selected modification type is not time type, resulting in node processing error",
			describeCN = "选择的修改类型不是时间类型，导致节点处理报错",
			dynamicDescription = "Expected modification type is date type, error table name: {}, field name: {}, type: {}, value: {}",
			dynamicDescriptionCN = "预期修改类型为日期类型，报错表名：{}，字段名：{}，类型：{}，值：{}",
			solution = "According to the table and field names prompted in the error message, check the task configuration to ensure that the type field is changed to time type and restart the task",
			solutionCN = "根据报错信息中提示的表名与字段名，检查任务配置，确保修改类型字段为时间类型后重新启动任务"
	)
	String SELECTED_TYPE_IS_NON_TIME = "17003";

}
