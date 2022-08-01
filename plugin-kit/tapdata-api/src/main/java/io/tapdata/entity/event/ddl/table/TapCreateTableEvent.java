package io.tapdata.entity.event.ddl.table;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.schema.TapTable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class TapCreateTableEvent extends TapTableEvent {
    public static final int TYPE = 206;
    private TapTable table;
    public TapCreateTableEvent table(TapTable table) {
        this.table = table;
        return this;
    }

    public TapCreateTableEvent() {
        super(TYPE);
    }

    public TapTable getTable() {
        return table;
    }

    public void setTable(TapTable table) {
        this.table = table;
    }

    @Override
    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if(tapEvent instanceof TapCreateTableEvent) {
            TapCreateTableEvent createTableEvent = (TapCreateTableEvent) tapEvent;
            createTableEvent.table = table;
        }
    }
}
