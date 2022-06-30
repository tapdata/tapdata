package io.tapdata.entity.event.control;

public class StopEvent extends ControlEvent {
    public static final int TYPE = 500;
    public StopEvent() {
        super(TYPE);
    }
}
