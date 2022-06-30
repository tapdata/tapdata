package io.tapdata.inspect.cdc.exception;

/**
 * 增量校验 - 运行配置异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/11/3 下午9:32 Create
 */
public class InspectCdcRunProfilesException extends RuntimeException {

	public InspectCdcRunProfilesException(String message) {
		super(message);
	}

	public InspectCdcRunProfilesException(String message, Throwable cause) {
		super(message, cause);
	}
}
