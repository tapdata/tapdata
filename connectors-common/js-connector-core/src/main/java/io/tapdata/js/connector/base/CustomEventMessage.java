package io.tapdata.js.connector.base;

import io.tapdata.entity.event.TapEvent;

public class CustomEventMessage {

    private TapEvent tapEvent;
    private Object contextMap;

    public TapEvent getTapEvent() {
        return tapEvent;
    }

    public CustomEventMessage tapEvent(TapEvent tapEvent) {
        this.tapEvent = tapEvent;
        return this;
    }

    public Object getContextMap() {
        return contextMap;
    }

    public CustomEventMessage contextMap(Object contextMap) {
        this.contextMap = contextMap;
        return this;
    }
}