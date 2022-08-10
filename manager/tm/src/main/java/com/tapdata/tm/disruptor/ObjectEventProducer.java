package com.tapdata.tm.disruptor;

import cn.hutool.core.lang.Assert;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import lombok.Data;

import java.util.List;

@Data
public class ObjectEventProducer {
    private RingBuffer<ObjectEvent> ringBuffer;

    public ObjectEventProducer(RingBuffer<ObjectEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void add(Object event) {
        Assert.notNull(event, "event is null");
        long sequence = this.ringBuffer.next();
        try {
            ObjectEvent objectEvent = this.ringBuffer.get(sequence);
            objectEvent.setEvent(event);
        } finally {
            this.ringBuffer.publish(sequence);
        }
    }

    public void addAll(List<Object> ts) {
        if (ts != null) {

            for (Object t : ts) {
                if (t != null) {
                    this.add(t);
                }
            }
        }
    }
}
