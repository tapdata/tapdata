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

	@TapExCode
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
}
