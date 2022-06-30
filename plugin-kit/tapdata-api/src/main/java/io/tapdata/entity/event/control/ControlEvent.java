package io.tapdata.entity.event.control;

import io.tapdata.entity.event.TapEvent;

public abstract class ControlEvent extends TapEvent {
    public ControlEvent(int type) {
        super(type);
    }
}
