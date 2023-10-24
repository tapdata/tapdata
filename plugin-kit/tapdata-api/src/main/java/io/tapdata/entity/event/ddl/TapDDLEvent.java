package io.tapdata.entity.event.ddl;


import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;


public abstract class TapDDLEvent extends TapBaseEvent {
    /**
     * origin DDL can be used when there is no change on ddl field on target side which is the same database with source.
     *
     * If any change on ddl field, the origin DDL shall be cleared by incremental engine, because the origin DDL may be incorrect any more.
     */
    protected Object originDDL;
    public TapDDLEvent(int type) {
        super(type);
    }

    public Object getOriginDDL() {
        return originDDL;
    }

    public void setOriginDDL(Object originDDL) {
        this.originDDL = originDDL;
    }

    @Override
    public void clone(TapEvent tapEvent) {
        super.clone(tapEvent);
        if(tapEvent instanceof TapDDLEvent) {
            TapDDLEvent baseEvent = (TapDDLEvent) tapEvent;
            baseEvent.originDDL = originDDL;
        }
    }
}
