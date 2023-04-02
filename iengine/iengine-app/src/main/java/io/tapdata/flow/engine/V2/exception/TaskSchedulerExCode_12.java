package io.tapdata.flow.engine.V2.exception;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 10:27
 **/
@TapExClass(code = 12, module = "Task Scheduler", prefix = "TSD", describe = "Schedule task start, stop")
public interface TaskSchedulerExCode_12 {
	@TapExCode
	String UNKNOWN_ERROR = "12001";
}
