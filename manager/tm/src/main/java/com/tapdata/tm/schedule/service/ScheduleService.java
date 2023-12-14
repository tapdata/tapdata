package com.tapdata.tm.schedule.service;


import cn.hutool.core.bean.BeanUtil;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.entity.TaskRecord;
import com.tapdata.tm.task.service.TaskRecordService;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class ScheduleService {

	@Autowired
	private TaskRecordService taskRecordService;

    public void executeTask(TaskDto taskDto) {
        WorkerService workerService = SpringContextHelper.getBean(WorkerService.class);
        UserService userService = SpringContextHelper.getBean(UserService.class);
        TaskService taskService = SpringContextHelper.getBean(TaskService.class);

        UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(taskDto.getUserId()));
        CalculationEngineVo calculationEngineVo = workerService.scheduleTaskToEngine(taskDto, userDetail, "task", taskDto.getName());
        int runningNum = calculationEngineVo.getRunningNum();
        if (taskService.checkIsCronOrPlanTask(taskDto)) {
            runningNum -= 1;
        }
        if (StringUtils.isNotBlank(taskDto.getAgentId()) &&  runningNum > calculationEngineVo.getTaskLimit()) {
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

            if (!TaskDto.TYPE_INITIAL_SYNC.equals(taskDto.getType()) && Lists.newArrayList(TaskDto.STATUS_STOP, TaskDto.STATUS_COMPLETE).contains(status)){
                taskService.renew(taskId, userDetail, true);
            }

            return;
        }
        if (TaskDto.STATUS_SCHEDULING.equals(status) || (TaskDto.STATUS_RUNNING.equals(status) && TaskDto.TYPE_INITIAL_SYNC.equals(taskDto.getType()))) {
            log.info("taskId {},status:{}  不用在进行全量任务", taskId, status);
            return;
        }
        if (scheduleDate < new Date().getTime()) {
					if (TaskDto.TYPE_INITIAL_SYNC.equals(taskDto.getType())) {
						TaskEntity taskSnapshot = new TaskEntity();
						BeanUtil.copyProperties(taskDto, taskSnapshot);
						taskSnapshot.setStatus(TaskDto.STATUS_RUNNING);
						taskSnapshot.setStartTime(new Date());
						ObjectId objectId = ObjectId.get();
						taskSnapshot.setTaskRecordId(objectId.toHexString());
						TaskRecord taskRecord = new TaskRecord(objectId.toHexString(), taskDto.getId().toHexString(), taskSnapshot, "system", new Date());
						// 创建记录
						taskRecordService.createRecord(taskRecord);
						taskDto.setTaskRecordId(objectId.toString());
						taskDto.setAttrs(new HashMap<>());
						taskDto.setScheduleDate(newScheduleDate);
						taskDto.setCrontabScheduleMsg("");
						taskService.save(taskDto, userDetail);
						// 执行记录
						taskService.start(taskDto.getId(), userDetail, true);
					} else if (TaskDto.TYPE_INITIAL_SYNC_CDC.equals(taskDto.getType()) && TaskDto.STATUS_RUNNING.equals(status)) {
                CompletableFuture<String> pause = CompletableFuture.supplyAsync(() -> {
                    taskService.pause(taskId, userDetail, false, false, true);
                    return "ok";
                });
                CompletableFuture<String> renew = pause.thenCompose(result -> CompletableFuture.supplyAsync(() -> {
                    performTaskWithSpin(taskId, TaskDto.STATUS_STOP, taskService);
                    taskService.renew(taskId, userDetail, true);
                    return "ok";
                })).exceptionally(ex -> {
                    log.error("schedule task pause error", ex);
                    return "error";
                });
                CompletableFuture<String> start = renew.thenCompose(result -> CompletableFuture.supplyAsync(() -> {
                    performTaskWithSpin(taskId, TaskDto.STATUS_WAIT_START, taskService);
                    taskService.start(taskId, userDetail, true);
                    return "ok";
                })).exceptionally(ex -> {
                    log.error("schedule renew pause error", ex);
                    return "error";
                });

                start.join();

            } else if (Lists.newArrayList(TaskDto.STATUS_STOP, TaskDto.STATUS_COMPLETE, TaskDto.STATUS_ERROR).contains(status)){
                CompletableFuture<String> renew = CompletableFuture.supplyAsync(() -> {
                    taskService.renew(taskId, userDetail, true);
                    return "ok";
                });
                CompletableFuture<String> start = renew.thenCompose(result -> CompletableFuture.supplyAsync(() -> {
                    performTaskWithSpin(taskId, TaskDto.STATUS_WAIT_START, taskService);
                    taskService.start(taskId, userDetail, true);
                    return "ok";
                })).exceptionally(ex -> {
                    log.error("schedule renew pause error", ex);
                    return "error";
                });

                start.join();
            } else if (TaskDto.STATUS_WAIT_START.equals(status)) {
                taskService.start(taskId, userDetail, true);
            } else {
                log.warn("other status can not run, need check taskId:{} status:{}", taskId, status);
            }
        }
    }

    /**
     * The state change is asynchronous, so spin has been added, with a maximum of 10 spins, each lasting 3 seconds
     * @param taskId
     * @param compareStatus
     * @param taskService
     */
    private void performTaskWithSpin(ObjectId taskId, String compareStatus, TaskService taskService) {
			TaskDto taskDto;
			String status;
			int maxAttempts = 10;
			int attempts = 0;
			do {
				taskDto = taskService.findByTaskId(taskId, "status");
				status = taskDto.getStatus();
				// If the status is not "OK", sleep for 3 seconds before the next attempt
				if (compareStatus.equals(status)) break;

				try {
					attempts++;
					Thread.sleep(3000);
				} catch (InterruptedException ignore) {
					break;
				}
			} while (attempts < maxAttempts);
		}

}


