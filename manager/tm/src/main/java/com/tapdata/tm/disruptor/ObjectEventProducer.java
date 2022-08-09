package com.tapdata.tm.disruptor;

import cn.hutool.core.lang.Assert;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import lombok.Data;

import java.util.List;

@Data
public class ObjectEventProducer<T> {
    private RingBuffer<ObjectEvent<T>> ringBuffer;

    public ObjectEventProducer(RingBuffer<ObjectEvent<T>> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void add(T event) {
        Assert.notNull(event, "event is null");
        long sequence = this.ringBuffer.next();
        try {
            ObjectEvent<T> objectEvent = this.ringBuffer.get(sequence);
            objectEvent.setEvent(event);
        } finally {
            this.ringBuffer.publish(sequence);
        }
    }

    public void addAll(List<T> ts) {
        if (ts != null) {

            for (T t : ts) {
                if (t != null) {
                    this.add(t);
                }
            }
        }
    }
}
