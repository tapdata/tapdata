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
			describe = "Init Connector state data failed",
			describeCN = "初始化数据源状态失败",
			dynamicDescription = "Connector state mapping name for initialization failure: {}, external storage name: {}",
			dynamicDescriptionCN = "初始化失败的数据源状态名称：{}，外存名称：{}",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. The external memory configuration corresponding to the shared mining task is found, and the connection test is carried out to see if it is available. If it is not available, the test failure information is used to troubleshoate. Restart the task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到主从合并节点配置的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动任务"
	)
	String INIT_PDK_STATE_MAP_FAILED = "28003";
}
