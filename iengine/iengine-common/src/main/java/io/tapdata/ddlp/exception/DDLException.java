package io.tapdata.ddlp.exception;

/**
 * DDL异常基类
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/16 上午12:51 Create
 */
public class DDLException extends RuntimeException {
	public DDLException() {
	}

	public DDLException(String message) {
		super(message);
	}

	public DDLException(String message, Throwable cause) {
		super(message, cause);
	}

	public DDLException(Throwable cause) {
		super(cause);
	}
}
