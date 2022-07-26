package io.tapdata.flow.engine.V2.exception.task;

import io.tapdata.flow.engine.V2.exception.FlowEngineException;

/**
 * start task exception
 *
 * @author jackin
 * @date 2021/12/1 9:18 PM
 **/
public class StartTaskException extends FlowEngineException {

	private static final long serialVersionUID = -412587046981234222L;

	public StartTaskException() {
		super();
	}

	public StartTaskException(String message) {
		super(message);
	}

	public StartTaskException(String message, Throwable cause) {
		super(message, cause);
	}

	public StartTaskException(Throwable cause) {
		super(cause);
	}
}
