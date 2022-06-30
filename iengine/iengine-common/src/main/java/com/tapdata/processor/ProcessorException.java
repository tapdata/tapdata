package com.tapdata.processor;

public class ProcessorException extends RuntimeException {

	private boolean needStop = false;

	public ProcessorException() {
		super();
	}

	public ProcessorException(String message, boolean needStop) {
		super(message);
		this.needStop = needStop;
	}

	public ProcessorException(String message, Throwable cause) {
		super(message, cause);
	}

	public ProcessorException(String message, Throwable cause, boolean needStop) {
		super(message, cause);
		this.needStop = needStop;
	}

	public ProcessorException(Throwable cause, boolean needStop) {
		super(cause);
		this.needStop = needStop;
	}

	public boolean isNeedStop() {
		return needStop;
	}
}
