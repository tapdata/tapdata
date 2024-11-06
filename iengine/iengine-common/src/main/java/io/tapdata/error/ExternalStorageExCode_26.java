package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

@TapExClass(code = 26, module = "External Storage", prefix = "ES", describe = "External Storage")
public interface ExternalStorageExCode_26 {
	@TapExCode
	String UNKNOWN_ERROR = "20001";

	@TapExCode(
			describe = "When the task is started, the available external storage configuration is searched. When the external storage database is initialized, no available external storage configuration is found",
			describeCN = "在启动任务时，会查找可用的外存配置进行初始化外存数据库时，找不到可用的外存配置",
			dynamicDescription = "An External Storage configuration named \"MongoDB External Storage\" is initialized by default when the system starts up. If no external storage configuration can be found, it is possible that the data in the intermediate library will be corrupted",
			dynamicDescriptionCN = "在系统启动时默认会初始化一个默认名为\"MongoDB External Storage\"的外存配置，如果无法找到任何外存配置时，有可能中间库的数据遭到破坏",
			solution = "Select System Administration -> External storage management from the Administration menu. enter the external storage management interface, click Create external storage. Fill in the information needed to create external storage, and click the connection test after saving. After successful saving, the external storage is set as the default storage",
			solutionCN = "在管理菜单中点击系统管理->外存管理，进到外存管理界面后，点击创建外存,填写创建外存需要的信息，并点击连接测试通过后保存。保存成功后将创建的外存需要设为默认外存"
	)
	String CANNOT_FOUND_EXTERNAL_STORAGE_CONFIG = "20002";
}
