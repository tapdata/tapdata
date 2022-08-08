package io.tapdata.aspect;

import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public class DropFieldFuncAspect extends DataFunctionAspect<DropFieldFuncAspect> {
	private TapDropFieldEvent dropFieldEvent;
	public DropFieldFuncAspect dropFieldEvent(TapDropFieldEvent dropFieldEvent) {
		this.dropFieldEvent = dropFieldEvent;
		return this;
	}
	private TapConnectorContext connectorContext;

	public DropFieldFuncAspect connectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
		return this;
	}

	public TapConnectorContext getConnectorContext() {
		return connectorContext;
	}

	public void setConnectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
	}

	public TapDropFieldEvent getDropFieldEvent() {
		return dropFieldEvent;
	}

	public void setDropFieldEvent(TapDropFieldEvent dropFieldEvent) {
		this.dropFieldEvent = dropFieldEvent;
	}
}
