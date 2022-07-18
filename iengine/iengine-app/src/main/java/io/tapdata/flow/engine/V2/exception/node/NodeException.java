package io.tapdata.flow.engine.V2.exception.node;

import io.tapdata.flow.engine.V2.exception.FlowEngineException;

/**
 * @author jackin
 * @date 2021/12/6 2:32 PM
 **/
public class NodeException extends FlowEngineException {

	public NodeException(String message) {
		super(message);
	}

	public NodeException(String message, Throwable cause) {
		super(message, cause);
	}

	public NodeException(Throwable cause) {
		super(cause);
	}

	public NodeException() {
	}
}
