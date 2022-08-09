package com.tapdata.tm.disruptor;

import com.lmax.disruptor.EventFactory;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class ObjectEventFactory<T> implements EventFactory<ObjectEvent<T>> {
    @Override
    public ObjectEvent<T> newInstance() {
        return new ObjectEvent<>();
    }
}
