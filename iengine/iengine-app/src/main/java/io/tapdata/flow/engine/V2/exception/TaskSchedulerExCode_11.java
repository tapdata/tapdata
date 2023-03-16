package io.tapdata.flow.engine.V2.exception;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 10:27
 **/
@TapExClass(code = 11, module = "Task Scheduler", describe = "Error code for iengine task scheduler")
public class TaskSchedulerExCode_11 {
	@TapExCode(
			describe = ""
	)
	public static final String CALL_STOP_TASK_API = "11001";
}
