package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

@TapExClass(code = 26, module = "External Storage", prefix = "ES", describe = "External Storage")
public interface ExternalStorageExCode_26 {
	@TapExCode
	String UNKNOWN_ERROR = "20001";

	@TapExCode(
			describe = "Unable to find available external storage configuration based on conditions",
			describeCN = "根据条件，无法找到可用的外存配置"
	)
	String CANNOT_FOUND_EXTERNAL_STORAGE_CONFIG = "20002";
}
