package com.tapdata.processor.error;

/**
 * @author samuel
 * @Description
 * @create 2024-07-25 11:05
 **/
public class FieldProcessRuntimeException extends RuntimeException {
	public FieldProcessRuntimeException() {
		super();
	}

	public FieldProcessRuntimeException(String message) {
		super(message);
	}

	public FieldProcessRuntimeException(String message, Throwable cause) {
		super(message, cause);
	}

	public FieldProcessRuntimeException(Throwable cause) {
		super(cause);
	}

	protected FieldProcessRuntimeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
