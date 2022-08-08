package io.tapdata.aspect;

import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public class NewFieldFuncAspect extends DataFunctionAspect<NewFieldFuncAspect> {
	private TapNewFieldEvent newFieldEvent;
	public NewFieldFuncAspect newFieldEvent(TapNewFieldEvent newFieldEvent) {
		this.newFieldEvent = newFieldEvent;
		return this;
	}
	private TapConnectorContext connectorContext;

	public NewFieldFuncAspect connectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
		return this;
	}

	public TapConnectorContext getConnectorContext() {
		return connectorContext;
	}

	public void setConnectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
	}

	public TapNewFieldEvent getNewFieldEvent() {
		return newFieldEvent;
	}

	public void setNewFieldEvent(TapNewFieldEvent newFieldEvent) {
		this.newFieldEvent = newFieldEvent;
	}
}
