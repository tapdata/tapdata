package com.tapdata.tm.disruptor.handler;

import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.disruptor.Element;
import com.tapdata.tm.task.bean.SyncTaskStatusDto;
import com.tapdata.tm.task.service.TaskRecordService;
import org.springframework.stereotype.Component;

/**
 * @author jiuyetx
 * @date 2022/9/6
 */
@Component
public class UpdateRecordStatusEventHandler implements BaseEventHandler<SyncTaskStatusDto, Boolean>{

    @Override
    public Boolean onEvent(Element<SyncTaskStatusDto> event, long sequence, boolean endOfBatch) {

        TaskRecordService taskRecordService = SpringUtil.getBean(TaskRecordService.class);
        taskRecordService.updateTaskStatus(event.getData());

        return true;
    }
}
