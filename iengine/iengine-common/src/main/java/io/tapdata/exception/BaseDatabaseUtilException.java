package io.tapdata.exception;

public class BaseDatabaseUtilException extends RuntimeException {

	public BaseDatabaseUtilException() {
		super();
	}

	public BaseDatabaseUtilException(String message) {
		super(message);
	}

	public BaseDatabaseUtilException(String message, Throwable cause) {
		super(message, cause);
	}

	public BaseDatabaseUtilException(Throwable cause) {
		super(cause);
	}

	protected BaseDatabaseUtilException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
