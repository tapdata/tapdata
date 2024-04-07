package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

@TapExClass(code = 33, module = "Engine",prefix = "ENG", describe = "Engine Not Support Feature")
public interface EngineExCode_33 {

    @TapExCode(
            describe = "The current version does not support this feature" ,
            describeCN = "当前版本不支持此功能"
    )
    String UNABLE_SUPPORT_FUNCTION_EXCEPTION = "33001";

	@TapExCode(
		describe = "Does not support this event type",
		describeCN = "不支持这个类型的事件处理"
	)
	String NOT_SUPPORT_RECORD_EVENT_TYPE_EXCEPTION = "33002";
}
