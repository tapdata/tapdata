package io.tapdata.entity.event.ddl;


import io.tapdata.entity.event.TapBaseEvent;


public abstract class TapDDLEvent extends TapBaseEvent {
    public TapDDLEvent(int type) {
        super(type);
    }
}
