package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapField;

import java.util.ArrayList;
import java.util.List;

public class TapNewFieldEvent extends TapFieldBaseEvent {
    public static final int TYPE = 209;
    private List<TapField> newFields;
    public TapNewFieldEvent field(TapField field) {
        if(newFields == null)
            newFields = new ArrayList<>();
        if(field != null)
            newFields.add(field);
        return this;
    }
    public TapNewFieldEvent() {
        super(TYPE);
    }

    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if (tapEvent instanceof TapNewFieldEvent) {
            TapNewFieldEvent newFieldEvent = (TapNewFieldEvent) tapEvent;
            if (newFields != null)
                newFieldEvent.newFields = new ArrayList<>(newFields);
        }
    }

    public List<TapField> getNewFields() {
        return newFields;
    }

    public void setNewFields(List<TapField> newFields) {
        this.newFields = newFields;
    }
}
