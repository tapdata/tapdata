package io.tapdata.exception;

/**
 * @author samuel
 * @Description
 * @create 2020-07-29 17:13
 **/
public class LogReaderException extends Exception {

	private boolean recoverable;

	public LogReaderException() {
		super();
	}

	public LogReaderException(String message, boolean recoverable) {
		super(message);
		this.recoverable = recoverable;
	}

	public LogReaderException(String message, Throwable cause, boolean recoverable) {
		super(message, cause);
		this.recoverable = recoverable;
	}

	public LogReaderException(Throwable cause) {
		super(cause);
	}

	protected LogReaderException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public boolean isRecoverable() {
		return recoverable;
	}
}
