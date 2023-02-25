package com.tapdata.tm.disruptor.service;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.tapdata.tm.disruptor.Element;
import com.tapdata.tm.disruptor.constants.DisruptorTopicEnum;
import com.tapdata.tm.disruptor.handler.DisruptorExceptionHandler;
import com.tapdata.tm.disruptor.handler.DistributeEventHandler;
import io.tapdata.common.executor.ExecutorsManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

@Service
@Slf4j
@Primary
@SuppressWarnings({"rawtypes"})
public class DisruptorService{

    private final DistributeEventHandler distributeEventHandler;
    private final DisruptorExceptionHandler disruptorExceptionHandler;
    private final ExecutorService executorService = ExecutorsManager.getInstance().getExecutorService();
    private Disruptor<Element> elementDisruptor;

    public DisruptorService(DistributeEventHandler distributeEventHandler, DisruptorExceptionHandler disruptorExceptionHandler) {
        this.distributeEventHandler = distributeEventHandler;
        this.disruptorExceptionHandler = disruptorExceptionHandler;
    }

    /**
     * 初始化disruptor
     */
    @PostConstruct
    @SuppressWarnings("unchecked")
    private void init() {
        // RingBuffer生产工厂,初始化RingBuffer的时候使用
        EventFactory<Element> factory = Element::new;
        // 阻塞策略
        BlockingWaitStrategy strategy = new BlockingWaitStrategy();
        // 指定RingBuffer的大小
        int bufferSize = 64;
        // 创建disruptor，采用单生产者模式
        elementDisruptor = new Disruptor<>(factory, bufferSize, executorService, ProducerType.SINGLE, strategy);
        // 设置EventHandler 并且后置清理消费过的数据
        elementDisruptor.handleEventsWith(distributeEventHandler);
        elementDisruptor.handleExceptionsFor(distributeEventHandler).with(disruptorExceptionHandler);
        // 启动disruptor的线程
        elementDisruptor.start();
    }

    /**
     * 发送消息
     *
     * @author ming
     * @date 2020-10-27 17:08
     */
    @SuppressWarnings("unchecked")
    public <T> void sendMessage(DisruptorTopicEnum disruptorTopicEnum, T data) {
        RingBuffer<Element> ringBuffer = elementDisruptor.getRingBuffer();
        // 获取下一个可用位置的下标
        long sequence = ringBuffer.next();
        try {
            // 返回可用位置的元素
            Element<T> event = ringBuffer.get(sequence);
            // 设置该位置元素的值
            event.setData(data);
            event.setTopic(disruptorTopicEnum);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * 销毁 disruptor队列
     *
     * @author ming
     * @date 2020-10-27 17:12
     */
    @PreDestroy
    public void destroy() {
        //销毁 disruptor
        elementDisruptor.shutdown();
    }
}
