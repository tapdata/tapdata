package com.tapdata.exception;

/**
 * @author samuel
 * @Description
 * @create 2024-04-19 11:17
 **/
public class CloneException extends RuntimeException {
	public CloneException(String message) {
		super(message);
	}

	public CloneException(String message, Throwable cause) {
		super(message, cause);
	}

	public CloneException(Throwable cause) {
		super(cause);
	}
}
