package com.tapdata.tm.disruptor.service.impl;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.tapdata.tm.disruptor.ObjectEvent;
import com.tapdata.tm.disruptor.ObjectEventFactory;
import com.tapdata.tm.disruptor.ObjectEventProducer;
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
public class  BasicEventServiceImpl implements BasicEventService {
    private static final int BUFFER_SIZE = 1024;
    private ObjectEventProducer disruptorQueue;

    private final AtomicLong eventCount = new AtomicLong();

    @PostConstruct
    private void init() {
        Disruptor<ObjectEvent> disruptor = new Disruptor<>(new ObjectEventFactory(),
                BUFFER_SIZE,
                new CustomizableThreadFactory("event-handler-"),
                ProducerType.SINGLE, new SleepingWaitStrategy());

        Consumer<Object> eventCountPrinter = o -> {
            long count = eventCount.incrementAndGet();
            log.info("receive [{}] event", count);
        };

        disruptor.handleEventsWith(new TaskRecordHandler<>(eventCountPrinter));

        disruptor.start();

        disruptorQueue = new ObjectEventProducer(disruptor.getRingBuffer());
    }

    @Override
    public void publish(Object event) {
        disruptorQueue.add(event);
    }

    @Override
    public long eventCount() {
        return eventCount.get();
    }
}
