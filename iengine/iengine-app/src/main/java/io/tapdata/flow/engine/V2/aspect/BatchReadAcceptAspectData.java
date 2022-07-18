package io.tapdata.flow.engine.V2.aspect;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.flow.engine.V2.entity.TapdataEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.List;

public class BatchReadAcceptAspectData extends DataNodeAspect<BatchReadAcceptAspectData> {
	private List<TapdataEvent> events;
	public BatchReadAcceptAspectData events(List<TapdataEvent> events) {
		this.events = events;
		return this;
	}
	private TapConnectorContext connectorContext;
	public BatchReadAcceptAspectData connectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
		return this;
	}
	private TapTable table;
	public BatchReadAcceptAspectData table(TapTable table) {
		this.table = table;
		return this;
	}
	private Object offsetState;
	public BatchReadAcceptAspectData offsetState(Object offsetState) {
		this.offsetState = offsetState;
		return this;
	}
	private int eventBatchSize;
	public BatchReadAcceptAspectData eventBatchSize(int eventBatchSize) {
		this.eventBatchSize = eventBatchSize;
		return this;
	}

	public TapConnectorContext getConnectorContext() {
		return connectorContext;
	}

	public void setConnectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
	}

	public TapTable getTable() {
		return table;
	}

	public void setTable(TapTable table) {
		this.table = table;
	}

	public Object getOffsetState() {
		return offsetState;
	}

	public void setOffsetState(Object offsetState) {
		this.offsetState = offsetState;
	}

	public int getEventBatchSize() {
		return eventBatchSize;
	}

	public void setEventBatchSize(int eventBatchSize) {
		this.eventBatchSize = eventBatchSize;
	}
	public List<TapdataEvent> getEvents() {
		return events;
	}

	public void setEvents(List<TapdataEvent> events) {
		this.events = events;
	}
}
