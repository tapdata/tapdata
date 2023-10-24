package io.tapdata.entity.event.ddl.field;

import io.tapdata.entity.schema.TapField;

public class InsertFieldItem extends TapFieldItem {
    public static final int TYPE = 101;
    private TapField field;

    public InsertFieldItem() {
        super(TYPE);
    }

    public InsertFieldItem field(TapField field) {
        this.field = field;
        return this;
    }

    public TapField getField() {
        return field;
    }

    public void setField(TapField field) {
        this.field = field;
    }
}
