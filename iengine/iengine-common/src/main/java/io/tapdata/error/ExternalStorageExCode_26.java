package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

@TapExClass(code = 26, module = "External Storage", prefix = "ES", describe = "External Storage")
public interface ExternalStorageExCode_26 {
	@TapExCode
	String UNKNOWN_ERROR = "20001";

	@TapExCode(
			describe = "Unable to find available external storage configuration based on conditions",
			describeCN = "在启动任务时，会根据系统管理->外存管理的外存配置初始化外存，当找不到可用的外存配置时，则会报此错误",
			solution = "Select System Administration -> External storage management from the Administration menu. enter the external storage management interface, click Create external storage. Fill in the information needed to create external storage, and click the connection test after saving. After successful saving, the external storage is set as the default storage",
			solutionCN = "1、在管理菜单中选择系统管理->外存管理，进到外存管理界面后，点击创建外存。填写创建外存需要的信息，并点击连接测试通过后保存。保存成功后将创建的外存设为默认外存"
	)
	String CANNOT_FOUND_EXTERNAL_STORAGE_CONFIG = "20002";
}
