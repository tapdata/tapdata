package com.tapdata.tm.disruptor.handler;

import cn.hutool.core.date.DateUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.google.common.collect.ImmutableMap;
import com.tapdata.tm.Settings.constant.AlarmKeyEnum;
import com.tapdata.tm.Settings.service.AlarmSettingService;
import com.tapdata.tm.alarm.constant.*;
import com.tapdata.tm.alarm.entity.AlarmInfo;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingDto;
import com.tapdata.tm.disruptor.Element;
import com.tapdata.tm.schedule.entity.ScheduleJobInfo;
import com.tapdata.tm.schedule.service.ScheduleJobService;
import com.tapdata.tm.task.bean.SyncTaskStatusDto;
import com.tapdata.tm.task.service.TaskRecordService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.QuartzManager;
import org.apache.commons.collections.CollectionUtils;
import org.quartz.JobDataMap;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

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
        TaskService taskService = SpringUtil.getBean(TaskService.class);
        AlarmSettingService alarmSettingService = SpringUtil.getBean(AlarmSettingService.class);
        QuartzManager quartzManager = SpringUtil.getBean(QuartzManager.class);

        String taskId = data.getTaskId();
        switch (data.getTaskStatus()) {
            case TaskDto.STATUS_STOP:
                // alarm stop task
//                if (alarmService.checkOpen(taskId, AlarmKeyEnum.TASK_STATUS_STOP.name(), "SYS")) {
//                    String stopSummary = MessageFormat.format(AlarmContentTemplate.TASK_STATUS_STOP_MANUAL, data.getUpdatorName(), DateUtil.now());
//
//                    AlarmInfo stopInfo = AlarmInfo.builder().status(AlarmStatusEnum.ING).level(AlarmLevelEnum.WARNING).component(AlarmComponentEnum.FE)
//                            .type(AlarmTypeEnum.SYNCHRONIZATIONTASK_ALARM).agnetId(data.getAgnetId()).taskId(taskId)
//                            .name(data.getTaskName()).summary(stopSummary).metric(AlarmKeyEnum.TASK_STATUS_STOP)
//                            .build();
//                    alarmService.save(stopInfo);
//                }

                // remove task quartz job
                ScheduleJobService jobService = SpringUtil.getBean(ScheduleJobService.class);
                List<ScheduleJobInfo> scheduleJobInfos = jobService.listByCode(taskId);

                if (CollectionUtils.isNotEmpty(scheduleJobInfos)) {
                    scheduleJobInfos.forEach(quartzManager::removeJob);
                }

                break;
            case TaskDto.STATUS_RUNNING:
                // start task quartz job
                TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId));
                if (Objects.nonNull(taskDto)) {
                    List<AlarmSettingDto> alarmSettingDtos = Lists.newArrayList();
                    alarmSettingDtos.addAll(taskDto.getAlarmSettings());

                    taskDto.getDag().getNodes().forEach(node -> {
                        if (node instanceof DatabaseNode) {
                            alarmSettingDtos.addAll(((DatabaseNode) node).getAlarmSettings());
                        } else if (node instanceof TableRenameProcessNode) {
                            alarmSettingDtos.addAll(((TableRenameProcessNode) node).getAlarmSettings());
                        }
                    });
                    if (CollectionUtils.isNotEmpty(alarmSettingDtos)) {
                        Map<String, AlarmSettingDto> collect = alarmSettingDtos.stream()
                                .filter(AlarmSettingDto::isOpen)
                                .collect(Collectors.toMap(AlarmSettingDto::getKey, Function.identity(), (e1, e2) -> e1));

                        List<AlarmSettingDto> alarmSettingList = alarmSettingService.findAll();
                        if (CollectionUtils.isNotEmpty(alarmSettingList)) {
                            alarmSettingList.forEach(t -> {
                                if (collect.containsKey(t.getKey()) && !t.isOpen()) {
                                    collect.remove(t.getKey());
                                }
                            });
                        }

                        if (!collect.isEmpty()) {
                            collect.forEach((k, v) -> {
                                AlarmKeyEnum alarmKeyEnum = AlarmKeyEnum.valueOf(k);
                                ScheduleJobInfo job = ScheduleJobInfo.builder().groupName(taskDto.getSyncType())
                                        .jobName(String.join("_", taskId, alarmKeyEnum.name()))
                                        .className(alarmKeyEnum.getClassName())
                                        .code(taskId).build();
                                JobDataMap jobDataMap = new JobDataMap();
                                jobDataMap.put("taskId", taskId);
                                jobDataMap.put("alarmSetting", v);
                                quartzManager.addJob(job, jobDataMap);
                            });
                        }

                    }
                }
                break;
            case TaskDto.STATUS_ERROR:
                // alarm error task
//                if (alarmService.checkOpen(taskId, AlarmKeyEnum.TASK_STATUS_ERROR.name(), "SYS")) {
//                    ScheduleJobInfo errorJob = ScheduleJobInfo.builder().groupName(TaskDto.SYNC_TYPE_MIGRATE)
//                            .jobName(String.join("_", taskId, AlarmKeyEnum.TASK_STATUS_ERROR.name()))
//                            .code(taskId).build();
//                    JobDataMap jobDataMap = new JobDataMap();
//                    jobDataMap.put("taskId", taskId);
//                    quartzManager.addJob(errorJob, jobDataMap);
//                }
                break;
        }
    }
}
