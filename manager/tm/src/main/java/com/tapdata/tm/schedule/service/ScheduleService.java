package com.tapdata.tm.schedule.service;


import cn.hutool.core.bean.BeanUtil;
import cn.hutool.extra.spring.SpringUtil;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.entity.TaskRecord;
import com.tapdata.tm.task.service.TaskRecordService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.CronUtil;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.quartz.*;
import org.springframework.stereotype.Service;

import java.util.Date;

@Service
@Slf4j
@DisallowConcurrentExecution
public class ScheduleService implements Job {



    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        TaskService taskService = SpringContextHelper.getBean(TaskService.class);
        UserService userService = SpringContextHelper.getBean(UserService.class);
        TaskRecordService taskRecordService = SpringUtil.getBean(TaskRecordService.class);
        JobKey jobKey = jobExecutionContext.getJobDetail().getKey();
        String jobId = jobKey.getName();
        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(jobId));
        // 防止任务被删除
        if(taskDto.is_deleted()){
            log.info("Taskid :" +jobId+" has be deleted" );
            CronUtil.removeJob(jobId);
            return;
        }
        // 修改任务状态
        if(StringUtils.isBlank(taskDto.getCrontabExpression()) || !taskDto.isPlanStartDateFlag()){
            log.info("Taskid :" +jobId+" has not schedule" );
            CronUtil.removeJob(jobId);
            return;
        }
        log.info("工作任务的名称:" + taskDto.getName());

        String status = taskDto.getStatus();
        // 防止错误或者没有释放掉任务，再次释放
        if (TaskDto.STATUS_ERROR.equalsIgnoreCase(status) ||
                TaskDto.STATUS_STOP.equalsIgnoreCase(status)) {
            taskService.deleteScheduleTask(taskDto);
            log.info("工作任务的名称:" + taskDto.getName() + " has stop ");
            return;
        }
        if (InspectStatusEnum.SCHEDULING.getValue().equals(status) || InspectStatusEnum.RUNNING.getValue().equals(status)) {
            log.info("taskId {},status:{}  不用在进行全量任务", jobId, status);
        } else {
            log.info("taskId {},status:{}  定时在全量任务", jobId, status);
            UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(taskDto.getUserId()));
            TaskEntity taskSnapshot = new TaskEntity();
            BeanUtil.copyProperties(taskDto, taskSnapshot);
            taskSnapshot.setStatus(TaskDto.STATUS_RUNNING);
            taskSnapshot.setStartTime(new Date());
            ObjectId objectId = ObjectId.get();
            taskSnapshot.setTaskRecordId(objectId.toString());
            TaskRecord taskRecord = new TaskRecord(objectId.toString(), taskDto.getId().toHexString(), taskSnapshot, taskDto.getUserId(), new Date());
            // 创建记录
            taskRecordService.createRecord(taskRecord);
            taskDto.setTaskRecordId(objectId.toString());
            taskService.save(taskDto,userDetail);
            // 执行记录
            taskService.startScheduleTask(taskDto, userDetail, "11");

        }
    }


}

