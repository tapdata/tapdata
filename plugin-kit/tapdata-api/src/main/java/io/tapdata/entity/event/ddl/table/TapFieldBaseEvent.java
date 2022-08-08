package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;

public abstract class TapFieldBaseEvent extends TapTableEvent {

    private Integer fieldPos;

    public TapFieldBaseEvent(int type) {
        super(type);
    }

    public TapFieldBaseEvent fieldPos(Integer fieldPos) {
        this.fieldPos = fieldPos;
        return this;
    }

    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if (tapEvent instanceof TapFieldBaseEvent) {
            TapFieldBaseEvent alterFieldDataTypeEvent = (TapFieldBaseEvent) tapEvent;
            if (fieldPos != null)
                alterFieldDataTypeEvent.fieldPos = fieldPos;
        }
    }

    public Integer getFieldPos() {
        return fieldPos;
    }

    public void setFieldPos(Integer fieldPos) {
        this.fieldPos = fieldPos;
    }
}
