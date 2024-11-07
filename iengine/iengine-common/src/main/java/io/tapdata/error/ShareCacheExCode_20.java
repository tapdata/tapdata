package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/4/18 11:55 Create
 */
@TapExClass(code = 20, module = "Share Cache", prefix = "SC", describe = "Share cache")
public interface ShareCacheExCode_20 {
	@TapExCode(
			describe = "Failed to write a cache record to storage\n",
			describeCN = "缓存记录写入外存时失败" ,
			dynamicDescription = "The configured external cache database is unable to provide normal service",
			dynamicDescriptionCN = "配置的外存缓存数据库无法正常提供服务",
			solution = "To check whether the external storage cache database is normal, you can click the sub-menu \"external storage Management\" in the parent menu \"System Management\" in the left menu bar of the system to enter the external storage management interface. The external memory configuration corresponding to the shared mining task is found, and the connection test is carried out to see if it is available. If it is not available, the test failure information is used to troubleshoate. Restart the shared mining task after the connection test passes",
			solutionCN = "检查外存缓存数据库是否正常，可以点击系统左侧菜单栏中的父菜单“系统管理”中的子菜单“外存管理”进入外存管理界面。找到共享挖掘任务对应的外存配置，进行连接测试看是否可用，如不可用则需要根据测试失败信息进行排查。待连接测试通过后重新启动共享挖掘任务"
	)
	String PDK_WRITE_SHARE_CACHE_FAILED = "20001";
	@TapExCode(
			describe = "Fetch the cache according to the cache name, when the incoming cache name encoding fails. This error will be thrown",
			describeCN = "根据缓存名称获取缓存，传入的缓存名称编码失败时。则会报此错误",
			dynamicDescription = "Cache name :{}",
			dynamicDescriptionCN = "缓存名称:{}",
			solution = "When retrieving the cache by name, the passed cache name needs to be UTF-8 encoded",
			solutionCN = "在根据缓存名称获取缓存时，传入的缓存名称需要能被UTF-8编码"
	)
	String ENCODE_CACHE_NAME = "20002";
}
