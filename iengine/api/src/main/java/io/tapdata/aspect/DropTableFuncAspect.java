package io.tapdata.aspect;

import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public class DropTableFuncAspect extends DataFunctionAspect<DropTableFuncAspect> {
	private TapDropTableEvent dropTableEvent;
	public DropTableFuncAspect dropTableEvent(TapDropTableEvent dropTableEvent) {
		this.dropTableEvent = dropTableEvent;
		return this;
	}
	private TapConnectorContext connectorContext;

	public DropTableFuncAspect connectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
		return this;
	}

	public TapConnectorContext getConnectorContext() {
		return connectorContext;
	}

	public void setConnectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
	}

	public TapDropTableEvent getDropTableEvent() {
		return dropTableEvent;
	}

	public void setDropTableEvent(TapDropTableEvent dropTableEvent) {
		this.dropTableEvent = dropTableEvent;
	}
}
