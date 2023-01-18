package com.tapdata.tm.disruptor.handler;

import cn.hutool.core.date.DateUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.google.common.collect.Maps;
import com.tapdata.tm.alarm.constant.AlarmComponentEnum;
import com.tapdata.tm.alarm.constant.AlarmContentTemplate;
import com.tapdata.tm.alarm.constant.AlarmStatusEnum;
import com.tapdata.tm.alarm.constant.AlarmTypeEnum;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.task.constant.AlarmKeyEnum;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.disruptor.Element;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.task.bean.SyncTaskStatusDto;
import com.tapdata.tm.task.service.TaskRecordService;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

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
        CompletableFuture.runAsync(() -> handlerStatusDoSomething(data));

        return true;
    }

    private void handlerStatusDoSomething(SyncTaskStatusDto data) {
        AlarmService alarmService = SpringUtil.getBean(AlarmService.class);

        String taskId = data.getTaskId();
        switch (data.getTaskStatus()) {
            case TaskDto.STATUS_STOP:
                String summary = MessageFormat.format(AlarmContentTemplate.TASK_STATUS_STOP_MANUAL, data.getUpdatorName(), DateUtil.now());
                Map<String, Object> param = Maps.newHashMap();
                param.put("updatorName", data.getUpdatorName());
                AlarmInfo alarmInfo = AlarmInfo.builder().status(AlarmStatusEnum.ING).level(Level.WARNING).component(AlarmComponentEnum.FE)
                        .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(data.getAgentId()).taskId(taskId)
                        .name(data.getTaskName()).summary(summary).metric(AlarmKeyEnum.TASK_STATUS_STOP)
                        .param(param)
                        .build();

                alarmInfo.setUserId(data.getUserId());
                alarmService.save(alarmInfo);

                break;
            case TaskDto.STATUS_RUNNING:
                alarmService.closeWhenTaskRunning(taskId);

                break;
            case TaskDto.STATUS_ERROR:
                String errorSummary = MessageFormat.format(AlarmContentTemplate.TASK_STATUS_STOP_ERROR, DateUtil.now());
                AlarmInfo errorInfo = AlarmInfo.builder().status(AlarmStatusEnum.ING).level(Level.EMERGENCY).component(AlarmComponentEnum.FE)
                        .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(data.getAgentId()).taskId(taskId)
                        .name(data.getTaskName()).summary(errorSummary).metric(AlarmKeyEnum.TASK_STATUS_ERROR)
                        .build();
                errorInfo.setUserId(data.getUserId());
                alarmService.save(errorInfo);
                break;
        }
    }
}
