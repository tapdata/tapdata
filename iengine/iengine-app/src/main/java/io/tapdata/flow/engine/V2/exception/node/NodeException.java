package io.tapdata.flow.engine.V2.exception.node;

import com.tapdata.entity.task.context.ProcessorBaseContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.flow.engine.V2.exception.FlowEngineException;

import java.util.ArrayList;
import java.util.List;

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
		// unwrap  cause NodeException into this
		if (cause instanceof NodeException) {
			NodeException nodeException = (NodeException) cause;
			this.context(nodeException.getContext()).events(nodeException.getEvents());
		}
	}

	public NodeException(Throwable cause) {
		super(cause);
	}

	public NodeException() {
	}

	private ProcessorBaseContext context;

	public NodeException context(ProcessorBaseContext context) {
		this.context = context;
		return this;
	}

	private List<TapEvent> events;

	public NodeException events(List<TapEvent> events) {
		if (null == this.events) {
			this.events = new ArrayList<>();
		}
		if (null == events) {
			return this;
		}
		for (TapEvent event : events) {
			if (null == event) {
				continue;
			}
			this.events.add(event);
		}
		return this;
	}

	public NodeException event(TapEvent event) {
		if (null == event) {
			return this;
		}

		if (null == this.events) {
			this.events = new ArrayList<>();
		}
		this.events.add(event);
		return this;
	}

	public ProcessorBaseContext getContext() {
		return context;
	}

	public List<TapEvent> getEvents() {
		return events;
	}
}
