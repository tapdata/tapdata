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
			describe = "The cache record fails to be written to the external memory. The reason is analyzed:\n" +
					"1. The external memory configured by the shared cache task is not available, resulting in a write failure",
			describeCN = "缓存记录写入外存时失败，原因分析：\n" +
					"1、共享缓存任务配置的外存不可用，导致写入失败",
			dynamicDescription = "",
			dynamicDescriptionCN = "",
			solution = "1. Check the reasons for the unavailability of the external database, and restart the cache task after the external database is restored",
			solutionCN = "1、排查外存数据库不可用的原因，需要将外存数据库恢复可用后重新启动缓存任务"
	)
	String PDK_WRITE_SHARE_CACHE_FAILED = "20001";
	@TapExCode(
			describe = "Fetch the cache according to the cache name, when the incoming cache name encoding fails. This error will be thrown",
			describeCN = "根据缓存名称获取缓存，传入的缓存名称编码失败时。则会报此错误",
			dynamicDescription = "Encoding the cache name {} in UTF-8  fails",
			dynamicDescriptionCN = "以UTF-8编码缓存名称:{} 失败",
			solution = "When retrieving the cache by name, the passed cache name needs to be UTF-8 encoded",
			solutionCN = "在根据缓存名称获取缓存时，传入的缓存名称需要能被UTF-8编码"
	)
	String ENCODE_CACHE_NAME = "20002";
}
