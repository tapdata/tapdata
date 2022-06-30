package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.schema.TapTable;

public abstract class TapTableEvent extends TapDDLEvent {

    public TapTableEvent(int type) {
        super(type);
    }
}
