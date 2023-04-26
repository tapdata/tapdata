package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/4/18 11:55 Create
 */
@TapExClass(code = 19, module = "Share Cache", prefix = "SC", describe = "Share cache")
public interface ShareCacheExCode_20 {
	@TapExCode(
			describe = "Write to share cache failed",
			describeCN = "写入共享缓存失败"
	)
	String PDK_WRITE_SHARE_CACHE_FAILED = "20001";
	@TapExCode(
			describe = "Encode cache name failed",
			describeCN = "缓存名编码失败"
	)
	String ENCODE_CACHE_NAME = "20002";
}
