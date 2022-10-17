package com.tapdata.tm.schedule;


import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.TaskResetEventDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskResetLogService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
public class TaskResetSchedule {

    @Autowired
    private TaskResetLogService taskResetLogService;

    @Autowired
    private TaskService taskService;

    @Autowired
    private UserService userService;


    @Scheduled(fixedDelay = 5 * 1000)
    @SchedulerLock(name ="checkTaskReset", lockAtMostFor = "60s", lockAtLeastFor = "60s")
    public void checkTaskReset() {
        checkNoResponseOp();
        resetRetry();
        cleanTaskResetTimes();
    }


    public void checkNoResponseOp() {
        try {
            long fiveMin = System.currentTimeMillis() - 5 * 1000;
            Date fiveMinDate = new Date(fiveMin);

            Criteria criteria = Criteria.where("time").lte(fiveMinDate);

            Query query = new Query(criteria);
            List<TaskResetEventDto> taskResetLogs = taskResetLogService.find(query);

            if (CollectionUtils.isEmpty(taskResetLogs)) {
                return;
            }

            Map<String, TaskResetEventDto> taskResetLogsMap = new HashMap<>();
            for (TaskResetEventDto taskResetLog : taskResetLogs) {
                TaskResetEventDto old = taskResetLogsMap.get(taskResetLog.getTaskId());
                if (old == null) {
                    taskResetLogsMap.put(taskResetLog.getTaskId(), taskResetLog);
                } else {
                    if (old.getTime().before(taskResetLog.getTime())) {
                        taskResetLogsMap.put(taskResetLog.getTaskId(), taskResetLog);
                    }
                }
            }

            taskResetLogs = new ArrayList<>(taskResetLogsMap.values());

            //需要将重置中的跟删除中的改成 重置失败或者删除失败
            for (TaskResetEventDto taskResetLog : taskResetLogs) {
                try {
                    ObjectId objectId = taskResetLog.getId();
                    TaskDto taskDto = taskService.checkExistById(objectId, "status");
                    if (TaskDto.STATUS_RENEWING.equals(taskDto.getStatus())) {
                        taskService.updateStatus(objectId, TaskDto.STATUS_RENEW_FAILED);
                    } else if (TaskDto.STATUS_DELETING.equals(taskDto.getStatus())) {
                        taskService.updateStatus(objectId, TaskDto.STATUS_DELETE_FAILED);
                    } else {
                        taskResetLogService.clearLogByTaskId(taskResetLog.getTaskId());
                    }
                } catch (Exception e) {
                    log.info("check reset no response, task id = {}", taskResetLog.getTaskId());
                }
            }
        } catch (Exception e) {
            log.warn("check task reset no response error");
        }
    }

    public void resetRetry() {
        try {
            //查询重置删除失败的任务，并且重试次数少于3次
            Criteria criteria = Criteria.where("status").in(TaskDto.STATUS_RENEW_FAILED, TaskDto.STATUS_DELETE_FAILED).and("is_deleted").ne(true)
                    .orOperator(Criteria.where("resetTimes").exists(false), Criteria.where("resetTimes").lt(3));
            Query query = new Query(criteria);
            List<TaskDto> taskDtos = taskService.findAll(query);
            if (CollectionUtils.isEmpty(taskDtos)) {
                return;
            }
            for (TaskDto taskDto : taskDtos) {
                try {
                    UserDetail user = userService.loadUserById(MongoUtils.toObjectId(taskDto.getUserId()));
                    int resetTimes = taskDto.getResetTimes() == null ? 0 : taskDto.getResetTimes();
                    Update update = Update.update("resetTimes", resetTimes + 1);
                    Query queryTask = new Query(Criteria.where("_id").is(taskDto.getId()));
                    taskService.update(queryTask, update);


                    if (TaskDto.STATUS_RENEW_FAILED.equals(taskDto.getStatus())) {
                        taskService.renew(taskDto.getId(), user);
                    } else if (TaskDto.STATUS_DELETE_FAILED.equals(taskDto.getStatus())) {
                        taskService.remove(taskDto.getId(), user);
                    }
                } catch (Exception e) {
                    log.info("reset retry error, task = {}", taskDto.getName());
                }

            }
        } catch (Exception e) {
            log.warn("all reset retry error");
        }
    }

    public void cleanTaskResetTimes() {
        try {
            Criteria taskCriteria = Criteria.where("status").nin(TaskDto.STATUS_RENEW_FAILED, TaskDto.STATUS_DELETE_FAILED, TaskDto.STATUS_RENEWING, TaskDto.STATUS_DELETING)
                    .and("is_deleted").ne(true).and("resetTimes").gt(0);
            Query query = new Query(taskCriteria);
            query.fields().include("_id", "name");
            List<TaskDto> taskDtos = taskService.findAll(query);
            for (TaskDto taskDto : taskDtos) {
                try {
                    Update update = Update.update("resetTimes", 0);
                    Query queryTask = new Query(Criteria.where("_id").is(taskDto.getId()));
                    taskService.update(queryTask, update);
                } catch (Exception e) {
                    log.info("clean task reset times error, task = {}", taskDto.getName());
                }
            }
        } catch (Exception e) {
            log.warn("clean all task reset times error");
        }
    }

}
