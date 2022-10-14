package com.tapdata.tm.disruptor.handler;

import com.alibaba.fastjson.JSON;
import com.lmax.disruptor.ExceptionHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author jiuyetx
 * @date 2022/9/6
 */
@Component
@Slf4j
public class DisruptorExceptionHandler<T> implements ExceptionHandler<T> {
    /**
     * <p>Strategy for handling uncaught exceptions when processing an event.</p>
     *
     * <p>If the strategy wishes to terminate further processing by the {@link BatchEventProcessor}
     * then it should throw a {@link RuntimeException}.</p>
     *
     * @param ex       the exception that propagated from the {@link EventHandler}.
     * @param sequence of the event which cause the exception.
     * @param event    being processed when the exception occurred.  This can be null.
     */
    @Override
    public void handleEventException(Throwable ex, long sequence, Object event) {
        log.error("处理disruptor事件异常,异常内容:{},编号:{},事件内容:{}", ex.getMessage(), sequence, JSON.toJSONString(event));
        ex.printStackTrace();
    }

    /**
     * Callback to notify of an exception during {@link LifecycleAware#onStart()}
     *
     * @param ex throw during the starting process.
     */
    @Override
    public void handleOnStartException(Throwable ex) {
        log.error("处理disruptor启动异常,异常内容:{}", ex.getMessage());
        ex.printStackTrace();
    }

    /**
     * Callback to notify of an exception during {@link LifecycleAware#onShutdown()}
     *
     * @param ex throw during the shutdown process.
     */
    @Override
    public void handleOnShutdownException(Throwable ex) {
        log.error("处理disruptor关闭异常,异常内容:{}", ex.getMessage());
        ex.printStackTrace();
    }
}
