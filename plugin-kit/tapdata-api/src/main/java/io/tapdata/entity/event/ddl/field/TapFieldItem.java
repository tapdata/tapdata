package io.tapdata.entity.event.ddl.field;

import io.tapdata.entity.schema.TapField;

public abstract class TapFieldItem {
    private int type;
    public TapFieldItem(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}
