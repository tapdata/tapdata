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
			describe = "The target model does not exist, there may be a problem with the model calculation",
			describeCN = "目标模型不存在，可能是模型推算出现问题",
			dynamicDescription = "Table name with empty model: {}, node name: {}",
			dynamicDescriptionCN = "模型为空的表名：{}，节点名：{}",
			solution = "According to the table name and node name indicated in the error message, find the corresponding master-slave configuration in the master-slave merge and set the matching condition of the embedded array to restart the task",
			solutionCN = "根据报错信息中提示的表名与节点名，确保该表存在，重新加载模型后启动任务"
	)
	String INIT_TARGET_TABLE_TAP_TABLE_NULL = "17002";

	@TapExCode(
			describe = "The selected modification type is non time type, resulting in node processing error",
			describeCN = "选择的修改类型为非时间类型，导致节点处理报错",
			dynamicDescription = "Expected modification type is date type, error table name: {}, field name: {}, type: {}, value: {}",
			dynamicDescriptionCN = "预期修改类型为日期类型，报错表名：{}，字段名：{}，类型：{}，值为：{}",
			solution = "According to the table name and node name indicated in the error message, find the corresponding master-slave configuration in the master-slave merge and set the matching condition of the embedded array to restart the task",
			solutionCN = "根据报错信息中提示的表名与节点名，确保修改类型为时间类型后重新启动任务"
	)
	String SELECTED_TYPE_IS_NON_TIME = "17003";

}
