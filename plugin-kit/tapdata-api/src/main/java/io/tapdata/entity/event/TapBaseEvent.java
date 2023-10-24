package io.tapdata.entity.event;

import io.tapdata.entity.utils.io.DataInputStreamEx;
import io.tapdata.entity.utils.io.DataOutputStreamEx;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public abstract class TapBaseEvent extends TapEvent {
    protected String associateId;
    protected String tableId;
    protected String exactlyOnceId;
    protected List<String> namespaces;
    /**
     * The reference time read from source, maybe some difference as sources are different
     * Used for CDC in most cases.
     * <p>
     * For example, MongoDB as source, when initial stage, referenceTime is null, when cdc stage, referenceTime is the clusterTime read from CDC stream
     */
    protected Long referenceTime;
    /*
    public void from(InputStream inputStream) throws IOException {
        super.from(inputStream);
        DataInputStreamEx dataInputStreamEx = dataInputStream(inputStream);
        associateId = dataInputStreamEx.readUTF();
        tableId = dataInputStreamEx.readUTF();
        referenceTime = dataInputStreamEx.readLong();
    }
    public void to(OutputStream outputStream) throws IOException {
        super.to(outputStream);
        DataOutputStreamEx dataOutputStreamEx = dataOutputStream(outputStream);
        dataOutputStreamEx.writeUTF(associateId);
        dataOutputStreamEx.writeUTF(tableId);
        dataOutputStreamEx.writeLong(referenceTime);
    }
    */
    public TapBaseEvent(int type) {
        super(type);
    }

//    protected String pdkId;
//    protected String pdkGroup;
//    protected String pdkVersion;

    @Override
    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if(tapEvent instanceof TapBaseEvent) {
            TapBaseEvent baseEvent = (TapBaseEvent) tapEvent;
            baseEvent.referenceTime = referenceTime;
//            baseEvent.pdkId = pdkId;
//            baseEvent.pdkGroup = pdkGroup;
//            baseEvent.pdkVersion = pdkVersion;
            baseEvent.tableId = tableId;
            baseEvent.namespaces = namespaces != null ? new ArrayList<>(namespaces) : null;
            baseEvent.associateId = associateId;
        }
    }

    public Long getReferenceTime() {
        return referenceTime;
    }

    public void setReferenceTime(Long referenceTime) {
        this.referenceTime = referenceTime;
    }

//    public String getPdkId() {
//        return pdkId;
//    }
//
//    public void setPdkId(String pdkId) {
//        this.pdkId = pdkId;
//    }
//
//    public String getPdkGroup() {
//        return pdkGroup;
//    }
//
//    public void setPdkGroup(String pdkGroup) {
//        this.pdkGroup = pdkGroup;
//    }
//
//    public String getPdkVersion() {
//        return pdkVersion;
//    }
//
//    public void setPdkVersion(String pdkVersion) {
//        this.pdkVersion = pdkVersion;
//    }

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public String getAssociateId() {
        return associateId;
    }

    public void setAssociateId(String associateId) {
        this.associateId = associateId;
    }

    public String tableMapKey() {
        return tableId + "@" + associateId;
    }

    public List<String> getNamespaces() {
        return namespaces;
    }

    public void setNamespaces(List<String> namespaces) {
        this.namespaces = namespaces;
    }

    public String getExactlyOnceId() {
        return exactlyOnceId;
    }

    public void setExactlyOnceId(String exactlyOnceId) {
        this.exactlyOnceId = exactlyOnceId;
    }

    //    @Override
//    public String toString() {
//        return "TapBaseEvent{" +
//                "associateId='" + associateId + '\'' +
//                ", tableId='" + tableId + '\'' +
//                ", referenceTime=" + referenceTime +
//                "} " + super.toString();
//    }
}
