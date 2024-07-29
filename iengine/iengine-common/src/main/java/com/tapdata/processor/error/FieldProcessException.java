package com.tapdata.processor.error;

/**
 * @author samuel
 * @Description
 * @create 2024-07-25 10:35
 **/
public class FieldProcessException extends Exception {
	public FieldProcessException() {
		super();
	}

	public FieldProcessException(String message) {
		super(message);
	}

	public FieldProcessException(String message, Throwable cause) {
		super(message, cause);
	}

	public FieldProcessException(Throwable cause) {
		super(cause);
	}

	protected FieldProcessException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
