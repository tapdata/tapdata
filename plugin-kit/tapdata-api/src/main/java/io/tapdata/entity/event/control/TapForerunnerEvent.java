package io.tapdata.entity.event.control;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapTable;

import java.util.List;

public class TapForerunnerEvent extends PatrolEvent {
    public static final int TYPE = 401;
    public TapForerunnerEvent() {
        super(TYPE);
    }
    private TapTable table;
    public TapForerunnerEvent table(TapTable table) {
        this.table = table;
        return this;
    }
    private String associateId;
    public TapForerunnerEvent associateId(String associateId) {
        this.associateId = associateId;
        return this;
    }
    private List<TapInsertRecordEvent> sampleRecords;
    public TapForerunnerEvent sampleRecords(List<TapInsertRecordEvent> sampleRecords) {
        this.sampleRecords = sampleRecords;
        return this;
    }

    @Override
    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if(tapEvent instanceof TapForerunnerEvent) {
            TapForerunnerEvent tapForerunnerEvent = (TapForerunnerEvent) tapEvent;
            tapForerunnerEvent.table = table; //TODO need copy?
            tapForerunnerEvent.associateId = associateId;
            tapForerunnerEvent.sampleRecords = sampleRecords; //TODO need copy?
        }
    }

    public TapTable getTable() {
        return table;
    }

    public void setTable(TapTable table) {
        this.table = table;
    }

    public String getAssociateId() {
        return associateId;
    }

    public void setAssociateId(String associateId) {
        this.associateId = associateId;
    }

    public List<TapInsertRecordEvent> getSampleRecords() {
        return sampleRecords;
    }

    public void setSampleRecords(List<TapInsertRecordEvent> sampleRecords) {
        this.sampleRecords = sampleRecords;
    }
}
