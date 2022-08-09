package com.tapdata.tm.disruptor.service.impl;

import com.lmax.disruptor.dsl.Disruptor;
import com.tapdata.tm.disruptor.*;
import com.tapdata.tm.disruptor.handler.TaskRecordHandler;
import com.tapdata.tm.disruptor.service.BasicEventService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Service
@Slf4j
public class  BasicEventServiceImpl<T> implements BasicEventService<T> {
    private static final int BUFFER_SIZE = 1024;
    private ObjectEventProducer<T> disruptorQueue;

    private final AtomicLong eventCount = new AtomicLong();

    @PostConstruct
    private void init() {
        Disruptor<ObjectEvent<T>> disruptor = new Disruptor<>(new ObjectEventFactory<T>(),
                BUFFER_SIZE,
                new CustomizableThreadFactory("event-handler-"));

        Consumer<?> eventCountPrinter = (Consumer<Object>) o -> {
            long count = eventCount.incrementAndGet();
            log.info("receive [{}] event", count);
        };

        disruptor.handleEventsWith(new TaskRecordHandler<T>(eventCountPrinter));

        disruptor.start();

        disruptorQueue = new ObjectEventProducer<>(disruptor.getRingBuffer());
    }

    @Override
    public void publish(T event) {
        disruptorQueue.add(event);
    }

    @Override
    public long eventCount() {
        return eventCount.get();
    }
}
