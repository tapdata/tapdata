package io.tapdata.debug;

public class DebugException extends Exception {
	public DebugException() {
		super();
	}

	public DebugException(String message) {
		super(message);
	}

	public DebugException(String message, Throwable cause) {
		super(message, cause);
	}

	public DebugException(Throwable cause) {
		super(cause);
	}

	protected DebugException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
