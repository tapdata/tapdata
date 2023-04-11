package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 19:20
 **/
@TapExClass(code = 16, module = "Task Merge Processor", prefix = "TMP", describe = "Task merge processor")
public interface TaskMergeProcessorExCode_16 {
	@TapExCode
	String UNKNOWN_ERROR = "16001";

	@TapExCode
	String FIND_BY_JOIN_KEY_FAILED = "16002";
}
