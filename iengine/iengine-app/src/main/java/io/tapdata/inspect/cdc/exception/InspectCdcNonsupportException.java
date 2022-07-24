package io.tapdata.inspect.cdc.exception;

/**
 * 增量校验不支持异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/11/4 下午4:37 Create
 */
public class InspectCdcNonsupportException extends RuntimeException {
	public InspectCdcNonsupportException() {
	}

	public InspectCdcNonsupportException(String message) {
		super(message);
	}

	public InspectCdcNonsupportException(String message, Throwable cause) {
		super(message, cause);
	}

	public InspectCdcNonsupportException(Throwable cause) {
		super(cause);
	}
}
