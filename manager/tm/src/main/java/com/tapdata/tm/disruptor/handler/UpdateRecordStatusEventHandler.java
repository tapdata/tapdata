package com.tapdata.tm.disruptor.handler;

import cn.hutool.core.date.DateUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.google.common.collect.Maps;
import com.tapdata.tm.alarm.constant.AlarmComponentEnum;
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
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MongoUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * @author jiuyetx
 * @date 2022/9/6
 */
@Component("updateRecordStatusEventHandler")
public class UpdateRecordStatusEventHandler implements BaseEventHandler<SyncTaskStatusDto, Boolean>{

    @Autowired
    private TaskService taskService;

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
        String taskName = data.getTaskName();
        String alarmDate = DateUtil.now();
        Map<String, Object> param = Maps.newHashMap();
        switch (data.getTaskStatus()) {
            case TaskDto.STATUS_RUNNING:
                alarmService.closeWhenTaskRunning(taskId);

                break;
            case TaskDto.STATUS_ERROR:
                TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId));
                boolean checkOpen = alarmService.checkOpen(taskDto, null, AlarmKeyEnum.TASK_STATUS_ERROR, null, data.getUserDetail());
                if (checkOpen) {
                    param.put("taskName", taskName);
                    param.put("alarmDate", alarmDate);
                    AlarmInfo errorInfo = AlarmInfo.builder().status(AlarmStatusEnum.ING).level(Level.EMERGENCY).component(AlarmComponentEnum.FE)
                            .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agentId(data.getAgentId()).taskId(taskId)
                            .name(data.getTaskName()).summary("TASK_STATUS_STOP_ERROR").metric(AlarmKeyEnum.TASK_STATUS_ERROR)
                            .param(param)
                            .build();
                    errorInfo.setUserId(taskDto.getUserId());
                    alarmService.save(errorInfo);
                }
                break;
        }
    }
}
