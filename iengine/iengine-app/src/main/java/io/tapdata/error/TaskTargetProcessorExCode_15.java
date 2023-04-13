package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-03-16 19:20
 **/
@TapExClass(code = 15, module = "Task Target Processor", prefix = "TTP", describe = "Task target processor")
public interface TaskTargetProcessorExCode_15 {
	@TapExCode
	String UNKNOWN_ERROR = "15001";

	@TapExCode(
			describe = "The target model does not exist, there may be a problem with the model calculation",
			describeCN = "目标模型不存在，可能是模型推算出现问题"
	)
	String INIT_TARGET_TABLE_TAP_TABLE_NULL = "15002";

	@TapExCode(
			describe = "Failed to create target table index",
			describeCN = "创建目标表索引失败"
	)
	String CREATE_INDEX_FAILED = "15003";

	@TapExCode(
			describe = "Failed to drop target table",
			describeCN = "删除目标表失败"
	)
	String DROP_TABLE_FAILED = "15004";

	@TapExCode(
			describe = "Automatic creation of target table failed",
			describeCN = "自动创建目标表失败"
	)
	String CREATE_TABLE_FAILED = "15005";

	@TapExCode(
			describe = "Failed to clear target table data",
			describeCN = "清空目标表数据失败"
	)
	String CLEAR_TABLE_FAILED = "15006";
}
