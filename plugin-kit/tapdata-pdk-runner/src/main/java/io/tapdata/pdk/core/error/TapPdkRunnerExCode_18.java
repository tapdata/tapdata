package io.tapdata.pdk.core.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 19:31
 **/
@TapExClass(code = 18, module = "Pdk Runner", prefix = "PKR", describe = "Pdk Runner")
public interface TapPdkRunnerExCode_18 {
	@TapExCode
	String UNKNOWN_ERROR = "18001";

	@TapExCode
	String CALL_ERROR_HANDLE_API_ERROR = "18002";
	@TapExCode(
			describe = "The exception class loader thrown from the PDK connector is incorrect, please confirm whether the scope of the pdk error code dependency is provided",
			describeCN = "数据源中抛出的异常类加载器不正确，请确认PDK错误码依赖是否范围是provided"
	)
	String EX_CLASS_LOADER_INVALID = "18003";
}
