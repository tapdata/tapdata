package io.tapdata.exception;

public class SourceException extends RuntimeException {

	private boolean needStop = false;

	public SourceException() {
		super();
	}

	public SourceException(String message, boolean needStop) {
		super(message);
		this.needStop = needStop;
	}

	public SourceException(String message, Throwable cause, boolean needStop) {
		super(message, cause);
		this.needStop = needStop;
	}

	public SourceException(Throwable cause, boolean needStop) {
		super(cause);
		this.needStop = needStop;
	}

	public boolean isNeedStop() {
		return needStop;
	}
}
