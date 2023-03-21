package io.tapdata.flow.engine.V2.exception;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 10:27
 **/
@TapExClass(code = 12, module = "Task Scheduler", prefix = "TSD", describe = "Error code for iengine task scheduler")
public interface TaskSchedulerExCode_12 {
	@TapExCode(
			describe = ""
	)
	String CALL_STOP_TASK_API = "11001";
}
