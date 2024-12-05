package com.tapdata.entity.task.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-06-01 19:23
 **/
@TapExClass(code = 23, module = "Task service", describe = "A service responsible for building and starting tasks", prefix = "TSV")
public interface TaskServiceExCode_23 {
	@TapExCode
	String UNKNOWN_ERROR = "23001";

	@TapExCode(
			describeCN = "初始化任务全局变量失败，任务ID不可为空",
			describe = "Failed to initialize task global variables, task ID cannot be empty"
	)
	String TASK_GLOBAL_VARIABLE_INIT_TASK_ID_EMPTY = "23002";

	@TapExCode(
			describeCN = "引擎推演模型失败",
			describe = "Engine deduction model failed",
			dynamicDescription = "Task name: {}, task ID: {}, task type: {}",
			dynamicDescriptionCN = "任务名：{}，任务ID：{}，任务类型：{}"
	)
	String TASK_FAILED_TO_LOAD_TABLE_STRUCTURE = "23004";
}
