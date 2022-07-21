package io.tapdata.exception;

public class ManagementException extends RuntimeException {

	private boolean needStop = false;

	public ManagementException() {
		super();
	}

	public ManagementException(String message) {
		super(message);
	}

	public ManagementException(String message, boolean needStop) {
		super(message);
		this.needStop = needStop;
	}

	public ManagementException(String message, Throwable cause, boolean needStop) {
		super(message, cause);
		this.needStop = needStop;
	}

	public ManagementException(String message, Throwable cause) {
		super(message, cause);
	}

	public ManagementException(Throwable cause, boolean needStop) {
		super(cause);
		this.needStop = needStop;
	}

	public boolean isNeedStop() {
		return needStop;
	}
}
