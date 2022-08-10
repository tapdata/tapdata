package com.tapdata.tm.disruptor.service;

public interface BasicEventService {
    void publish(Object event);

    long eventCount();
}
