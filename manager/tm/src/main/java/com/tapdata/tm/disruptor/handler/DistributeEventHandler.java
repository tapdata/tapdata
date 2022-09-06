package com.tapdata.tm.disruptor.handler;

import com.lmax.disruptor.EventHandler;
import com.tapdata.tm.disruptor.Element;
import com.tapdata.tm.disruptor.constants.DisruptorTopicEnum;
import com.tapdata.tm.task.bean.SyncTaskStatusDto;
import com.tapdata.tm.task.entity.TaskRecord;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 基础event handler
 * 根据topic 分发数据
 * @author jiuyetx
 * @date 2022/9/6
 */
@Component
@Setter(onMethod_ = {@Autowired})
public class DistributeEventHandler implements EventHandler {

    private CreateRecordEventHandler createRecordEventHandler;
    private UpdateRecordStatusEventHandler updateRecordStatusEventHandler;

    @Override
    @SuppressWarnings("unchecked")
    public void onEvent(Object event, long sequence, boolean endOfBatch) throws Exception {
        if (!(event instanceof Element)) {
            throw new RuntimeException("类型错误,必须为Element类型~");
        }
        Element<?> element = (Element<?>) event;
        //分发topic  到对应的handler
        if (element.getTopic() == DisruptorTopicEnum.CREATE_RECORD) {
            createRecordEventHandler.onEvent((Element<TaskRecord>) event, sequence, endOfBatch);
        } else if (element.getTopic() == DisruptorTopicEnum.TASK_STATUS) {
            updateRecordStatusEventHandler.onEvent((Element<SyncTaskStatusDto>) event, sequence, endOfBatch);
        } else {
            throw new RuntimeException("topic未注册!无法分发消息");
        }
    }
}
