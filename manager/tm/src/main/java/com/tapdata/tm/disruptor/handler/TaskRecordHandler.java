package com.tapdata.tm.disruptor.handler;

import com.tapdata.tm.disruptor.ObjectEvent;
import com.tapdata.tm.task.bean.SyncTaskStatusDto;
import com.tapdata.tm.task.entity.TaskRecord;
import com.tapdata.tm.task.service.TaskRecordService;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.function.Consumer;

@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TaskRecordHandler<T> extends ObjectEventHandler<T> {

    private TaskRecordService taskRecordService;
    public TaskRecordHandler(Consumer<?> consumer) {
        super(consumer);
    }

    public void onEvent(ObjectEvent event, long sequence, boolean endOfBatch) {
        log.info("sequence [{}], endOfBatch [{}], event : {}", sequence, endOfBatch, event);

        Object obj = event.getEvent();
        if (obj instanceof TaskRecord) {
            TaskRecord data = (TaskRecord) obj;
            taskRecordService.createRecord(data);
        } else if (obj instanceof SyncTaskStatusDto) {

        } else {

        }

        if (super.consumer != null) {
            super.consumer.accept(null);
        }
    }
}
