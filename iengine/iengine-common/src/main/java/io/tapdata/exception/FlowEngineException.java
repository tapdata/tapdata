package io.tapdata.exception;

import java.io.Serializable;

/**
 * base flow engine exception
 *
 * @author jackin
 * @date 2021/12/1 9:16 PM
 **/
public class FlowEngineException extends RuntimeException implements Serializable {

	private static final long serialVersionUID = 1134678962286126653L;

	public FlowEngineException() {
	}

	public FlowEngineException(final String message) {
		super(message);
	}

	public FlowEngineException(final String message, final Throwable cause) {
		super(message, cause);
	}

	public FlowEngineException(final Throwable cause) {
		super(cause);
	}
}
