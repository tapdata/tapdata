package com.tapdata.tm.disruptor.handler;

import cn.hutool.extra.spring.SpringUtil;
import com.lmax.disruptor.EventHandler;
import com.tapdata.tm.disruptor.Element;
import org.springframework.stereotype.Component;

/**
 * 基础event handler
 * 根据topic 分发数据
 * @author jiuyetx
 * @date 2022/9/6
 */
@Component
@SuppressWarnings("rawtypes")
public class DistributeEventHandler implements EventHandler {

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void onEvent(Object event, long sequence, boolean endOfBatch) {
        if (!(event instanceof Element)) {
            throw new RuntimeException("类型错误,必须为Element类型~");
        }
        Element<?> element = (Element<?>) event;
        //分发topic  到对应的handler
        BaseEventHandler handler = SpringUtil.getBean(element.getTopic().getBeanName(), BaseEventHandler.class);
        handler.onEvent(element, sequence, endOfBatch);
    }
}
