package io.tapdata.exception;

public class MongodbException extends Exception {

	public MongodbException() {
		super();
	}

	public MongodbException(String message) {
		super(message);
	}

	public MongodbException(String message, Throwable cause) {
		super(message, cause);
	}

	public MongodbException(Throwable cause) {
		super(cause);
	}

	protected MongodbException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
