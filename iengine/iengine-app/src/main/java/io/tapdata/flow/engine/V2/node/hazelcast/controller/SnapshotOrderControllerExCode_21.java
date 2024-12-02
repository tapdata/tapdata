package io.tapdata.flow.engine.V2.node.hazelcast.controller;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;


/**
 * @author samuel
 * @Description
 * @create 2023-05-11 11:40
 **/
@TapExClass(code = 21, module = "Snapshot Order Controller", describe = "Scheduling multi-node initialization order", prefix = "SOC")
public interface SnapshotOrderControllerExCode_21 {
	@TapExCode
	String UNKNOWN_ERROR = "21001";

	@TapExCode(
			describe = "Snapshot runner occurs an error",
			describeCN = "全量读取出现错误"
	)
	String RUNNER_ERROR = "21002";

	@TapExCode(
			describe = "Detected illegal node controller status",
			describeCN = "识别到非法的节点控制器状态",
			dynamicDescription = "Unable to execute subsequent state flow, status is: {}",
			dynamicDescriptionCN = "无法执行后续状态流转，状态为：{}"
	)
	String NONSUPPORT_STATUS = "21003";

	@TapExCode
	String NODE_CONTROLLER_NOT_FOUND = "21004";

	@TapExCode
	String NODE_CONTROL_LAYER_NOT_FOUND = "21005";

	@TapExCode(
			describe = "Failed to create snapshot order controller, task data cannot be null",
			describeCN = "创建初始化顺序控制器失败，任务数据不能为空"
	)
	String CREATE_CONTROLLER_TASK_NULL = "21006";

	@TapExCode(
			describe = "Failed to create snapshot order controller, task data cannot be null",
			describeCN = "创建初始化顺序控制器失败，任务数据不能为空"
	)
	String FAILED_TO_CREATE_CONTROLLER_TASK_NULL = "21007";

	@TapExCode(
			describe = "The format of the initialization sequence controller was incorrect. Procedure",
			describeCN = "初始化顺序控制器格式错误",
			dynamicDescription = "The incorrect sequence controller format is：{}\n" +
					"Sequence controller content：{}",
			dynamicDescriptionCN = "错误的顺序控制器格式为：{}\n" +
					"顺序控制器内容：{}"
	)
	String SNAPSHOT_ORDER_LIST_FORMAT_ERROR = "21008";
	@TapExCode(
			describe = "The master-slave merging mode has changed",
			describeCN = "主从合并模式发生变更",
			dynamicDescription = "Last merge mode: {}, current merge mode: {}",
			dynamicDescriptionCN = "上次合并模式：{}，当前合并模式：{}",
			solution = "After modifying the master-slave merge mode, reset the task and run it again",
			solutionCN = "修改主从合并模式后，重置任务再次运行"
	)
	String CANNOT_CHANGE_MERGE_MODE_WITH_OUT_RESET = "21009";
}
