package com.tapdata.tm.disruptor;

import com.lmax.disruptor.EventFactory;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class ObjectEventFactory implements EventFactory<ObjectEvent> {
    @Override
    public ObjectEvent newInstance() {
        return new ObjectEvent();
    }
}
