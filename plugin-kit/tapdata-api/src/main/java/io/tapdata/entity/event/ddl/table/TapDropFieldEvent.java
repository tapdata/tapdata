package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapField;

import java.util.ArrayList;
import java.util.List;

public class TapDropFieldEvent extends TapTableEvent {
    public static final int TYPE = 306;
    private List<String> fields;
    public TapDropFieldEvent field(String field) {
        if(fields == null)
            fields = new ArrayList<>();
        if(field != null)
            fields.add(field);
        return this;
    }

    public TapDropFieldEvent() {
        super(TYPE);
    }

    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if (tapEvent instanceof TapDropFieldEvent) {
            TapDropFieldEvent dropFieldEvent = (TapDropFieldEvent) tapEvent;
            if (fields != null)
                dropFieldEvent.fields = new ArrayList<>(fields);
        }
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }
}
