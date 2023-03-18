package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 19:20
 **/
@TapExClass(code=11, module = "IEngine", describe = "IEngine Common Error Code")
public interface TapIEngineExCode_11 {
	@TapExCode
	String UNKNOWN_ERROR = "11001";
}
