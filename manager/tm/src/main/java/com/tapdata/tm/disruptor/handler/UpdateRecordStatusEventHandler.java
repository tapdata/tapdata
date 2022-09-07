package com.tapdata.tm.disruptor.handler;

import cn.hutool.core.date.DateUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.alarm.constant.*;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.disruptor.Element;
import com.tapdata.tm.task.bean.SyncTaskStatusDto;
import com.tapdata.tm.task.service.TaskRecordService;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;

/**
 * @author jiuyetx
 * @date 2022/9/6
 */
@Component("updateRecordStatusEventHandler")
public class UpdateRecordStatusEventHandler implements BaseEventHandler<SyncTaskStatusDto, Boolean>{

    @Override
    public Boolean onEvent(Element<SyncTaskStatusDto> event, long sequence, boolean endOfBatch) {

        TaskRecordService taskRecordService = SpringUtil.getBean(TaskRecordService.class);
        SyncTaskStatusDto data = event.getData();
        taskRecordService.updateTaskStatus(data);

        if (data.getTaskStatus().equals(TaskDto.STATUS_STOP)) {
            String summary = MessageFormat.format(AlarmContentTemplate.TASK_STATUS_STOP_MANUAL, data.getUpdatorName(), DateUtil.now());

            AlarmInfo info = AlarmInfo.builder().status(AlarmStatusEnum.ING).level(AlarmLevelEnum.WARNING).component(AlarmComponentEnum.FE)
                    .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agnetId(data.getAgnetId()).taskId(data.getTaskId())
                    .name(data.getTaskName()).summary(summary).build();

            AlarmService alarmService = SpringUtil.getBean(AlarmService.class);
        }

        return true;
    }
}
