package io.tapdata.pdk.core.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 19:31
 **/
@TapExClass(code = 13, module = "Pdk Runner")
public interface TapPdkRunnerExCode_13 {
	@TapExCode
	String UNKNOWN_ERROR = "13001";

	@TapExCode
	String CALL_ERROR_HANDLE_API_ERROR = "13002";
}
