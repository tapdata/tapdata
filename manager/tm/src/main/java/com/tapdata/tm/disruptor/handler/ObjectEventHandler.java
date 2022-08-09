package com.tapdata.tm.disruptor.handler;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WorkHandler;
import com.tapdata.tm.disruptor.ObjectEvent;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;

@Slf4j
public abstract class ObjectEventHandler<T> implements EventHandler<ObjectEvent<T>> {

    public final Consumer<?> consumer;

    public ObjectEventHandler(Consumer<?> consumer) {
        this.consumer = consumer;
    }

    public void onEvent(ObjectEvent<T> event, long sequence, boolean endOfBatch) {
        log.info("sequence [{}], endOfBatch [{}], event : {}", sequence, endOfBatch, event);

        if (consumer != null) {
            consumer.accept(null);
        }
    }

}
