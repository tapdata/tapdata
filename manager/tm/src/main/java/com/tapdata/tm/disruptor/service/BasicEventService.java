package com.tapdata.tm.disruptor.service;

/**
 * @author liujiaxin
 */
public interface BasicEventService {
    /** 创建事件
     * @param event 消息内容
     */
    void publish(Object event);

    /**
     * 消息数量
     * @return 消息数量
     */
    long eventCount();
}
