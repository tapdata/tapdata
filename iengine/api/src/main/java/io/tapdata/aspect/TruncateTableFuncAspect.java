package io.tapdata.aspect;

import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public class TruncateTableFuncAspect extends DataFunctionAspect<TruncateTableFuncAspect>{
    private TapClearTableEvent clearTableEvent;
    public TruncateTableFuncAspect truncateTableEvent(TapClearTableEvent clearTableEvent) {
        this.clearTableEvent = clearTableEvent;
        return this;
    }
    private TapConnectorContext connectorContext;

    public TruncateTableFuncAspect connectorContext(TapConnectorContext connectorContext) {
        this.connectorContext = connectorContext;
        return this;
    }

    public TapConnectorContext getConnectorContext() {
        return connectorContext;
    }

    public void setConnectorContext(TapConnectorContext connectorContext) {
        this.connectorContext = connectorContext;
    }

    public TapClearTableEvent getClearTableEvent() {
        return clearTableEvent;
    }

    public void setClearTableEvent(TapClearTableEvent clearTableEvent) {
        this.clearTableEvent = clearTableEvent;
    }
}
