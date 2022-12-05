package com.tapdata.tm.schedule.service;


import cn.hutool.core.bean.BeanUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.entity.TaskRecord;
import com.tapdata.tm.task.service.TaskRecordService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.TriggerBuilder;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.HashMap;

@Service
@Slf4j
public class ScheduleService{


    public void executeTask(TaskDto taskDto) {
        TaskService taskService = SpringContextHelper.getBean(TaskService.class);
        UserService userService = SpringContextHelper.getBean(UserService.class);
        TaskRecordService taskRecordService = SpringUtil.getBean(TaskRecordService.class);
        // 防止任务被删除
        if (taskDto.is_deleted()) {
            log.info("Taskid :" + taskDto.getId() + " has be deleted");
            return;
        }
        // 修改任务状态
        if (StringUtils.isBlank(taskDto.getCrontabExpression()) || !taskDto.isCrontabExpressionFlag()) {
            log.info("Taskid :" + taskDto.getId() + " has not schedule");
            return;
        }
        String status = taskDto.getStatus();
        log.info("工作任务的名称:" + taskDto.getName());

        // 防止错误或者没有释放掉任务，再次释放
//        if (TaskDto.STATUS_ERROR.equalsIgnoreCase(status) ||
//                TaskDto.STATUS_STOP.equalsIgnoreCase(status)) {
//            log.info("工作任务的名称:" + taskDto.getName() + " has stop ");
//            return;
//        }
        CronTrigger cronTrigger = TriggerBuilder.newTrigger().withIdentity("Caclulate Date")
                .withSchedule(CronScheduleBuilder.cronSchedule(taskDto.getCrontabExpression())).build();
        Date startTime = cronTrigger.getStartTime();
        Long newScheduleDate = cronTrigger.getFireTimeAfter(startTime).getTime();
        Long scheduleDate = taskDto.getScheduleDate();
        UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(taskDto.getUserId()));
        if (scheduleDate == null || newScheduleDate < scheduleDate) {
            taskDto.setScheduleDate(newScheduleDate);
            taskService.save(taskDto, userDetail);
            return;
        }
        if (TaskDto.STATUS_SCHEDULING.equals(status) || TaskDto.STATUS_RUNNING.equals(status)) {
            log.info("taskId {},status:{}  不用在进行全量任务", taskDto.getId(), status);
            return;
        }
        if (scheduleDate < new Date().getTime()) {
            log.info("taskId {},status:{}  定时在全量任务", taskDto.getId(), status);
            TaskEntity taskSnapshot = new TaskEntity();
            BeanUtil.copyProperties(taskDto, taskSnapshot);
            taskSnapshot.setStatus(TaskDto.STATUS_RUNNING);
            taskSnapshot.setStartTime(new Date());
            ObjectId objectId = ObjectId.get();
            taskSnapshot.setTaskRecordId(objectId.toHexString());
            TaskRecord taskRecord = new TaskRecord(objectId.toHexString(), taskDto.getId().toHexString(), taskSnapshot, taskDto.getUserId(), new Date());
            // 创建记录
            taskRecordService.createRecord(taskRecord);
            taskDto.setTaskRecordId(objectId.toString());
            taskDto.setAttrs(new HashMap<>());
            taskDto.setScheduleDate(newScheduleDate);
            taskService.save(taskDto, userDetail);
            // 执行记录
            taskService.start(taskDto.getId(), userDetail);

        }
    }

}


