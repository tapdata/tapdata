package io.tapdata.flow.engine.V2.aspect;

import io.tapdata.flow.engine.V2.entity.TapdataEvent;

import java.util.List;

public class BatchReadNodeAspect extends NodeAspect<BatchReadNodeAspect> {
	private List<TapdataEvent> events;
	public BatchReadNodeAspect events(List<TapdataEvent> events) {
		this.events = events;
		return this;
	}

	public List<TapdataEvent> getEvents() {
		return events;
	}

	public void setEvents(List<TapdataEvent> events) {
		this.events = events;
	}
}
