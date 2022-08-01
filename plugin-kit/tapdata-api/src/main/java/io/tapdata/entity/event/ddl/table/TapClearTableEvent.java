package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.schema.TapTable;

import java.util.ArrayList;

public class TapClearTableEvent extends TapTableEvent {
    public static final int TYPE = 205;
    public TapClearTableEvent() {
        super(TYPE);
    }

    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
    }
}
