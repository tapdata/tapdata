package io.tapdata.flow.engine.V2.aspect;

import io.tapdata.flow.engine.V2.entity.TapdataEvent;

import java.util.List;

public class StreamReadNodeAspect extends NodeAspect<StreamReadNodeAspect> {
	private List<TapdataEvent> events;
	public StreamReadNodeAspect events(List<TapdataEvent> events) {
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
