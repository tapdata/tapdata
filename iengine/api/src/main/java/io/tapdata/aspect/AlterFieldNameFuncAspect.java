package io.tapdata.aspect;

import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public class AlterFieldNameFuncAspect extends DataFunctionAspect<AlterFieldNameFuncAspect> {
	private TapAlterFieldNameEvent alterFieldNameEvent;
	public AlterFieldNameFuncAspect alterFieldNameEvent(TapAlterFieldNameEvent alterFieldNameEvent) {
		this.alterFieldNameEvent = alterFieldNameEvent;
		return this;
	}
	private TapConnectorContext connectorContext;

	public AlterFieldNameFuncAspect connectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
		return this;
	}

	public TapConnectorContext getConnectorContext() {
		return connectorContext;
	}

	public void setConnectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
	}

	public TapAlterFieldNameEvent getAlterFieldNameEvent() {
		return alterFieldNameEvent;
	}

	public void setAlterFieldNameEvent(TapAlterFieldNameEvent alterFieldNameEvent) {
		this.alterFieldNameEvent = alterFieldNameEvent;
	}
}
