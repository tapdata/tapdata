package io.tapdata.inspect.cdc.exception;

/**
 * 增量校验 - 配置异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/11/4 下午8:45 Create
 */
public class InspectCdcConfException extends RuntimeException {
	public InspectCdcConfException() {
	}

	public InspectCdcConfException(String message) {
		super(message);
	}

	public static void throwIfTrue(boolean isThrow, String msg) {
		if (isThrow) throw new InspectCdcConfException(msg);
	}
}
