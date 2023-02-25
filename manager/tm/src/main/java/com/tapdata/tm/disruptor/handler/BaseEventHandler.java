package com.tapdata.tm.disruptor.handler;

import com.tapdata.tm.disruptor.Element;

/**
 * @author jiuyetx
 * @date 2022/9/6
 */
public interface BaseEventHandler<T, R> {

    R onEvent(Element<T> event, long sequence, boolean endOfBatch);
}
