package io.tapdata.aspect;

import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;

public class TruncateTableFuncAspect extends DataFunctionAspect<TruncateTableFuncAspect>{
    private TapClearTableEvent truncateTableEvent;
    public TruncateTableFuncAspect truncateTableEvent(TapClearTableEvent truncateTableEvent) {
        this.truncateTableEvent = truncateTableEvent;
        return this;
    }
    private TapConnectorContext connectorContext;

    public TruncateTableFuncAspect connectorContext(TapConnectorContext connectorContext) {
        this.connectorContext = connectorContext;
        return this;
    }

    public TapClearTableEvent getTruncateTableEvent() {
        return truncateTableEvent;
    }

    public TapConnectorContext getConnectorContext() {
        return connectorContext;
    }

    public void setConnectorContext(TapConnectorContext connectorContext) {
        this.connectorContext = connectorContext;
    }


}
