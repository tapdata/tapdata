package io.tapdata.entity.event.ddl.index;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.schema.TapIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class TapIndexEvent extends TapDDLEvent {

    public TapIndexEvent(int type) {
        super(type);
    }
}
