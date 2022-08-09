package com.tapdata.tm.disruptor.service;

import com.tapdata.tm.disruptor.ObjectEvent;

public interface BasicEventService<T> {
    void publish(T event);

    long eventCount();
}
