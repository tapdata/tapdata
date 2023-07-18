package com.tapdata.tm.schedule.service;


import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.SpringContextHelper;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.TriggerBuilder;
import org.springframework.stereotype.Service;

import java.util.Date;

@Service
@Slf4j
public class ScheduleService{

    public void executeTask(TaskDto taskDto) {
        WorkerService workerService = SpringContextHelper.getBean(WorkerService.class);
        UserService userService = SpringContextHelper.getBean(UserService.class);
        TaskService taskService = SpringContextHelper.getBean(TaskService.class);

        UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(taskDto.getUserId()));
        CalculationEngineVo calculationEngineVo = workerService.scheduleTaskToEngine(taskDto, userDetail, "task", taskDto.getName());
        if (StringUtils.isNotBlank(taskDto.getAgentId()) && calculationEngineVo.getRunningNum() > calculationEngineVo.getTaskLimit()) {
            // 调度失败
            taskDto.setCrontabScheduleMsg("Task.ScheduleLimit");
            taskService.save(taskDto, userDetail);
            return;
        }

        // 防止任务被删除
        ObjectId taskId = taskDto.getId();
        if (taskDto.is_deleted()) {
            log.info("Taskid :" + taskId + " has be deleted");
            return;
        }
        // 修改任务状态
        if (StringUtils.isBlank(taskDto.getCrontabExpression()) || taskDto.getCrontabExpressionFlag() == null || !taskDto.getCrontabExpressionFlag()) {
            log.info("Taskid :" + taskId + " has not schedule");
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
        if (scheduleDate == null || newScheduleDate < scheduleDate) {
            taskDto.setScheduleDate(newScheduleDate);
            taskDto.setCrontabScheduleMsg("");
            taskService.save(taskDto, userDetail);

            if (Lists.newArrayList(TaskDto.STATUS_STOP, TaskDto.STATUS_COMPLETE).contains(status)){
                taskService.renew(taskId, userDetail);
            }

            return;
        }
        if (TaskDto.STATUS_SCHEDULING.equals(status) || (TaskDto.STATUS_RUNNING.equals(status) && TaskDto.TYPE_INITIAL_SYNC.equals(taskDto.getType()))) {
            log.info("taskId {},status:{}  不用在进行全量任务", taskId, status);
            return;
        }
        if (scheduleDate < new Date().getTime()) {

            if (TaskDto.TYPE_INITIAL_SYNC_CDC.equals(taskDto.getType()) && TaskDto.STATUS_RUNNING.equals(status)) {
                taskService.pause(taskId, userDetail, false);
            } else if (Lists.newArrayList(TaskDto.STATUS_STOP, TaskDto.STATUS_COMPLETE).contains(status)){
                taskService.renew(taskId, userDetail);
            } else if (TaskDto.STATUS_WAIT_START.equals(status)) {
                taskService.start(taskId, userDetail, true);
            } else {
                log.warn("other status can not run, need check taskId:{} status:{}", taskId, status);
            }
        }
    }

}


