package io.tapdata.cdc.ddl.exception;

/**
 * DDL 异常基类
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/11 下午8:27 Create
 */
public class DdlException extends RuntimeException {
	public DdlException() {
	}

	public DdlException(String message) {
		super(message);
	}

	public DdlException(String message, Throwable cause) {
		super(message, cause);
	}

	public DdlException(Throwable cause) {
		super(cause);
	}
}
