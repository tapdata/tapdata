package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 19:20
 **/
@TapExClass(code = 27, module = "Task Inspect", prefix = "TIN", describe = "Task Inspect")
public interface TaskInspectExCode_27 {
	@TapExCode
	String UNKNOWN_ERROR = "27001";

	@TapExCode
	String TABLE_NO_EXISTS = "27002";

	@TapExCode
	String CONNECTOR_NOT_SUPPORT_FUNCTION = "27003";

	@TapExCode
	String PARAM_ERROR = "27004";

	@TapExCode
	String COMMAND_COUNT_PARAM_ERROR = "27005";

	@TapExCode
	String AUTO_RECOVERY_DUPLICATE = "27006";

	@TapExCode
	String AUTO_RECOVERY_NOT_EXISTS = "27007";

    @TapExCode
    String AUTO_RECOVERY_CLIENT_DUPLICATE = "27008";

	@TapExCode
	String AUTO_RECOVERY_TIMEOUT = "27009";

	@TapExCode
	String AUTO_RECOVERY_NOT_FOUND_RECOVERY_TYPE = "27010";
}
