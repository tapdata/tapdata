package io.tapdata.exception;

/**
 * @author samuel
 * @Description
 * @create 2022-04-21 14:43
 **/
public class HazelcastNotExistsException extends RuntimeException {
	public HazelcastNotExistsException() {
		super();
	}

	public HazelcastNotExistsException(String message) {
		super(message);
	}

	public HazelcastNotExistsException(String message, Throwable cause) {
		super(message, cause);
	}

	public HazelcastNotExistsException(Throwable cause) {
		super(cause);
	}

	protected HazelcastNotExistsException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
