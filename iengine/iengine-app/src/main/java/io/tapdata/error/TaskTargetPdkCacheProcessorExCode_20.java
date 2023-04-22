package io.tapdata.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/4/18 11:55 Create
 */
@TapExClass(code = 19, module = "Target PDK Cache Processor", prefix = "TPCP", describe = "Share cache write processor")
public interface TaskTargetPdkCacheProcessorExCode_20 {
	@TapExCode(
			describe = "Write to share cache failed",
			describeCN = "写入共享缓存失败"
	)
	String WRITE_SHARE_CACHE_FAILED = "20001";
}
