package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 19:20
 **/
@TapExClass(code = 17, module = "Task Date Processor", prefix = "TDP", describe = "Task date processor")
public interface TaskDateProcessorExCode_17 {
	@TapExCode
	String UNKNOWN_ERROR = "17001";

	@TapExCode(
			describe = "The target model does not exist, there may be a problem with the model calculation",
			describeCN = "目标模型不存在，可能是模型推算出现问题"
	)
	String INIT_TARGET_TABLE_TAP_TABLE_NULL = "15002";
}
