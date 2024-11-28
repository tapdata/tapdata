package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author samuel
 * @Description
 * @create 2023-11-30 15:59
 **/
@TapExClass(code = 28, module = "Pdk State Map", prefix = "PSM")
public interface PdkStateMapExCode_28 {
	@TapExCode
	String UNKNOWN_ERROR = "28001";
	@TapExCode(
			describe = "Write Tap Info into PDK State Map failed"
	)
	String INSERT_TAPDATA_INFO_FAILED = "28002";
	@TapExCode(
			describe = "Init PDK state map failed",
			describeCN = "初始化PDK状态映射失败",
			dynamicDescription = "PDK state mapping name for initialization failure: {}",
			dynamicDescriptionCN = "初始化失败的PDK状态映射名称：{}")
	String INIT_PDK_STATE_MAP_FAILED = "28003";
}
