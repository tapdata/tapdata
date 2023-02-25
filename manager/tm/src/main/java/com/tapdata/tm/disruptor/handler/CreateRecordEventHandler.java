package com.tapdata.tm.disruptor.handler;

import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.disruptor.Element;
import com.tapdata.tm.task.entity.TaskRecord;
import com.tapdata.tm.task.service.TaskRecordService;
import org.springframework.stereotype.Component;

/**
 * @author jiuyetx
 * @date 2022/9/6
 */
@Component("createRecordEventHandler")
public class CreateRecordEventHandler implements BaseEventHandler<TaskRecord, Boolean>{

    @Override
    public Boolean onEvent(Element<TaskRecord> event, long sequence, boolean endOfBatch) {

        TaskRecordService taskRecordService = SpringUtil.getBean(TaskRecordService.class);
        taskRecordService.createRecord(event.getData());

        SpringUtil.getBean(AlarmService.class).delAlarm(event.getData().getTaskId());
        return true;
    }
}
