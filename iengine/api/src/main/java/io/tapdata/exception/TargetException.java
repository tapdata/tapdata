package io.tapdata.exception;

import io.tapdata.common.exception.TapdataException;
import io.tapdata.common.logging.error.ErrorCodeEnum;

public class TargetException extends TapdataException {

	private boolean needStop = false;

	public TargetException() {
		super();
	}

	public TargetException(Throwable cause) {
		super(cause);
	}

	protected TargetException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public TargetException(String message) {
		super(message);
	}

	public TargetException(String message, boolean needStop) {
		super(message);
		this.needStop = needStop;
	}

	public TargetException(String message, Throwable cause) {
		super(message, cause);
	}

	public TargetException(boolean needStop) {
		this.needStop = needStop;
	}

	public TargetException(boolean needStop, String message) {
		super(message);
		this.needStop = needStop;
	}

	public TargetException(boolean needStop, String message, Throwable throwable) {
		super(message, throwable);
		this.needStop = needStop;
	}

	public boolean isNeedStop() {
		return needStop;
	}

	public TargetException configErrorCode(ErrorCodeEnum errorCode) {
		this.errorCode = errorCode;
		return this;
	}
}
