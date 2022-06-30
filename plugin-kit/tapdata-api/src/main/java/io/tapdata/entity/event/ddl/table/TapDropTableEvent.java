package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.schema.TapTable;

public class TapDropTableEvent extends TapTableEvent {
    public static final int TYPE = 203;
    public TapDropTableEvent() {
        super(TYPE);
    }
}
