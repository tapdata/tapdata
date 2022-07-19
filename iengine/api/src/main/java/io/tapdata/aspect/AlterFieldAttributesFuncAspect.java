package io.tapdata.aspect;

import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public class AlterFieldAttributesFuncAspect extends DataFunctionAspect<AlterFieldAttributesFuncAspect> {
	private TapAlterFieldAttributesEvent alterFieldAttributesEvent;
	public AlterFieldAttributesFuncAspect alterFieldAttributesEvent(TapAlterFieldAttributesEvent alterFieldAttributesEvent) {
		this.alterFieldAttributesEvent = alterFieldAttributesEvent;
		return this;
	}
	private TapConnectorContext connectorContext;

	public AlterFieldAttributesFuncAspect connectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
		return this;
	}

	public TapConnectorContext getConnectorContext() {
		return connectorContext;
	}

	public void setConnectorContext(TapConnectorContext connectorContext) {
		this.connectorContext = connectorContext;
	}

	public TapAlterFieldAttributesEvent getAlterFieldAttributesEvent() {
		return alterFieldAttributesEvent;
	}

	public void setAlterFieldAttributesEvent(TapAlterFieldAttributesEvent alterFieldAttributesEvent) {
		this.alterFieldAttributesEvent = alterFieldAttributesEvent;
	}
}
