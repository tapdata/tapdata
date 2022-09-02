package com.tapdata.tm.disruptor.handler;

import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.disruptor.ObjectEvent;
import com.tapdata.tm.task.bean.SyncTaskStatusDto;
import com.tapdata.tm.task.entity.TaskRecord;
import com.tapdata.tm.task.service.TaskRecordService;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;

@Slf4j
public class TaskRecordHandler<T> extends ObjectEventHandler<T> {

    public TaskRecordHandler(Consumer<?> consumer) {
        super(consumer);
    }

    public void onEvent(ObjectEvent event, long sequence, boolean endOfBatch) {
        log.info("sequence [{}], endOfBatch [{}], event : {}", sequence, endOfBatch, event);

        TaskRecordService taskRecordService = SpringUtil.getBean(TaskRecordService.class);

        Object obj = event.getEvent();
        if (obj instanceof TaskRecord) {
            taskRecordService.createRecord((TaskRecord) obj);
        } else if (obj instanceof SyncTaskStatusDto) {
            taskRecordService.updateTaskStatus((SyncTaskStatusDto) obj);
        } else {

        }

        if (super.consumer != null) {
            super.consumer.accept(null);
        }
    }
}
