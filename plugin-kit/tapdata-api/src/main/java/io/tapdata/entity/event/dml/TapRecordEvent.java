package io.tapdata.entity.event.dml;

import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;

public abstract class TapRecordEvent extends TapBaseEvent {
    /**
     * 数据源的类型， mysql一类
     */
    protected String connector;
    /**
     * 数据源的版本
     */
    protected String connectorVersion;

    public TapRecordEvent(int type) {
        super(type);
    }

    @Override
    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if(tapEvent instanceof TapRecordEvent) {
            TapRecordEvent recordEvent = (TapRecordEvent) tapEvent;
            recordEvent.connector = connector;
            recordEvent.connectorVersion = connectorVersion;
        }
    }

    public String getConnector() {
        return connector;
    }

    public void setConnector(String connector) {
        this.connector = connector;
    }

    public String getConnectorVersion() {
        return connectorVersion;
    }

    public void setConnectorVersion(String connectorVersion) {
        this.connectorVersion = connectorVersion;
    }

    @Override
    public String toString() {
        return "TapRecordEvent{" +
                "connector='" + connector + '\'' +
                ", connectorVersion='" + connectorVersion + '\'' +
                "} " + super.toString();
    }
}
