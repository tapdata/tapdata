package io.tapdata.entity.event.dml;

import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;

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
    /*
    public void from(InputStream inputStream) throws IOException {
        super.from(inputStream);
        DataInputStreamEx dataInputStreamEx = dataInputStream(inputStream);
        connector = dataInputStreamEx.readUTF();
        connectorVersion = dataInputStreamEx.readUTF();
    }
    public void to(OutputStream outputStream) throws IOException {
        super.to(outputStream);
        DataOutputStreamEx dataOutputStreamEx = dataOutputStream(outputStream);
        dataOutputStreamEx.writeUTF(connector);
        dataOutputStreamEx.writeUTF(connectorVersion);
    }
    */
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

    public abstract Map<String, Object> getFilter(Collection<String> primaryKeys);
//    @Override
//    public String toString() {
//        return "TapRecordEvent{" +
//                "connector='" + connector + '\'' +
//                ", connectorVersion='" + connectorVersion + '\'' +
//                "} " + super.toString();
//    }
}
