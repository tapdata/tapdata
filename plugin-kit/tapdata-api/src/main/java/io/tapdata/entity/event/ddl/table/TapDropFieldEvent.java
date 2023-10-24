package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;

import java.util.ArrayList;
import java.util.List;

public class TapDropFieldEvent extends TapFieldBaseEvent {
    public static final int TYPE = 207;
    private String fieldName;
    public TapDropFieldEvent fieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public TapDropFieldEvent() {
        super(TYPE);
    }

    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if (tapEvent instanceof TapDropFieldEvent) {
            TapDropFieldEvent dropFieldEvent = (TapDropFieldEvent) tapEvent;
            if (fieldName != null)
                dropFieldEvent.fieldName = fieldName;
        }
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public String toString() {
        return "TapDropFieldEvent{" +
                "tableId='" + tableId + '\'' +
                ", fieldName='" + fieldName + '\'' +
                '}';
    }
}
