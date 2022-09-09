package com.tapdata.tm.schedule.job;

import cn.hutool.core.date.DateUtil;
import com.alibaba.fastjson.JSON;
import com.tapdata.tm.Settings.constant.AlarmKeyEnum;
import com.tapdata.tm.alarm.constant.*;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.autoinspect.service.TaskAutoInspectResultsService;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import lombok.Setter;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.Objects;

/**
 * @author jiuyetx
 * @date 2022/9/6
 */
@Component
@Setter(onMethod_ = {@Autowired})
public class TaskInspectErrorJob implements Job {

    private TaskService taskService;
    private TaskAutoInspectResultsService inspectResultsService;
    private AlarmService alarmService;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        JobDataMap dataMap = jobExecutionContext.getMergedJobDataMap();
        String taskId = dataMap.getString("taskId");
        AlarmSettingDto alarmSetting = JSON.parseObject(JSON.toJSONString(dataMap.get("alarmSetting")), AlarmSettingDto.class);

        long count = inspectResultsService.countByTaskId(taskId);
        TaskDto data = taskService.findById(MongoUtils.toObjectId(taskId));

        if (count > 0) {
            String summary = MessageFormat.format(AlarmContentTemplate.TASK_INSPECT_ERROR, DateUtil.now());

                AlarmInfo alarmInfo = AlarmInfo.builder().status(AlarmStatusEnum.ING).level(AlarmLevelEnum.WARNING)
                        .component(AlarmComponentEnum.FE)
                        .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agnetId(data.getAgentId()).taskId(taskId)
                        .name(data.getName()).summary(summary).metric(AlarmKeyEnum.TASK_STATUS_STOP)
                        .build();
                alarmService.save(alarmInfo);

        }
    }
}
