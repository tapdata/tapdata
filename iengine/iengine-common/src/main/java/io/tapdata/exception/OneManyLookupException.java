package io.tapdata.exception;

public class OneManyLookupException extends RuntimeException {

	private boolean retryException = false;

	public OneManyLookupException() {
		super();
	}

	public OneManyLookupException(String message) {
		super(message);
	}

	public OneManyLookupException(String message, Throwable cause) {
		super(message, cause);
	}

	public OneManyLookupException(String message, Throwable cause, boolean retryException) {
		super(message, cause);
		this.retryException = retryException;
	}

	public OneManyLookupException(Throwable cause) {
		super(cause);
	}

	protected OneManyLookupException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public boolean isRetryException() {
		return retryException;
	}
}
