package com.tapdata.tm.schedule;


import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.TaskResetEventDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.service.TaskResetLogService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
public class TaskResetSchedule {

    @Autowired
    private TaskResetLogService taskResetLogService;

    @Autowired
    private TaskService taskService;

    @Autowired
    private UserService userService;

    @Value("${task.reset.times: 2}")
    private int resetAllTimes;


    @Value("${task.reset.timeoutInterval: 50}")
    private int timeoutInterval;

    @Autowired
    private StateMachineService stateMachineService;


    @Scheduled(fixedDelayString = "30000")
    @SchedulerLock(name ="checkTaskReset", lockAtMostFor = "120s", lockAtLeastFor = "60s")
    public void checkTaskReset() {
        checkNoResponseOp();
        resetRetry();
        cleanTaskResetTimes();
    }


    /**
     * @see com.tapdata.tm.statemachine.enums.DataFlowEvent#RENEW_DEL_FAILED
     */
    public void checkNoResponseOp() {

        try {
            Criteria taskCri = Criteria.where("status").in(TaskDto.STATUS_RENEWING, TaskDto.STATUS_DELETING).and("is_deleted").ne(true);

            Query query1 = new Query(taskCri);
            //query1.fields().include("status", "last_updated", "user_id");
            List<TaskDto> taskDtos = taskService.findAll(query1);
            List<String> taskIds = taskDtos.stream().map(t -> t.getId().toHexString()).distinct().collect(Collectors.toList());
            Criteria criteria1 = Criteria.where("taskId").in(taskIds);
            Query query2 = new Query(criteria1);
            List<TaskResetEventDto> taskResetEventDtos = taskResetLogService.find(query2);
            Map<String, List<TaskResetEventDto>> taskLogMap = taskResetEventDtos.stream()
                    .collect(Collectors.groupingBy(TaskResetEventDto::getTaskId));


            List<TaskDto> updateTasks = new ArrayList<>();
            List<TaskDto> successTasks = new ArrayList<>();
            List<TaskDto> failedTasks = new ArrayList<>();
            long curr = System.currentTimeMillis();

            int timeout = timeoutInterval * 1000;
            for (TaskDto taskDto : taskDtos) {
                List<TaskResetEventDto> taskResetEvents = taskLogMap.get(taskDto.getId().toHexString());
                if (CollectionUtils.isNotEmpty(taskResetEvents)) {
                    List<TaskResetEventDto.ResetStatusEnum> enums = taskResetEvents.stream().map(TaskResetEventDto::getStatus)
                            .distinct().collect(Collectors.toList());
                    if (enums.contains(TaskResetEventDto.ResetStatusEnum.TASK_SUCCEED)) {
                        successTasks.add(taskDto);
                        continue;
                    } else if (enums.contains(TaskResetEventDto.ResetStatusEnum.TASK_FAILED)) {
                        failedTasks.add(taskDto);
                        continue;
                    }

                    TaskResetEventDto taskResetEventDto = taskResetEvents.stream().max(Comparator.comparing(TaskResetEventDto::getTime)).get();
                    if (curr - taskResetEventDto.getTime().getTime() >= timeout) {
                        updateTasks.add(taskDto);
                    }

                } else {
                    if (curr - taskDto.getLastUpdAt().getTime() >= timeout) {
                        updateTasks.add(taskDto);
                    }
                }

            }
            List<String> userLists = updateTasks.stream().map(BaseDto::getUserId).collect(Collectors.toList());
            Set<String> successUserSet = successTasks.stream().map(BaseDto::getUserId).collect(Collectors.toSet());
            Set<String> failedUserSet = failedTasks.stream().map(BaseDto::getUserId).collect(Collectors.toSet());
            userLists.addAll(successUserSet);
            userLists.addAll(failedUserSet);

            Map<String, UserDetail> userDetailMap = userService.getUserMapByIdList(userLists);

            for (TaskDto taskDto : updateTasks) {
                try {

                    UserDetail user = userDetailMap.get(taskDto.getUserId());
                    if (user == null) {
                        continue;
                    }


                    if (TaskDto.STATUS_RENEWING.equals(taskDto.getStatus()) || TaskDto.STATUS_DELETING.equals(taskDto.getStatus())) {
                        StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.OVERTIME, user);
                        if (stateMachineResult.isOk()) {
                            Query query = Query.query(Criteria.where("_id").is(taskDto.getId()));
                            Update update = Update.update("last_updated", new Date());
                            taskService.update(query, update);
                        }

                        //taskService.updateStatus(taskDto.getId(), TaskDto.STATUS_RENEW_FAILED);
                    }  else {
                        taskResetLogService.clearLogByTaskId(taskDto.getId().toHexString());
                    }
                } catch (Exception e) {
                    log.info("check reset no response, task id = {}", taskDto.getId().toHexString());
                }
            }

            for (TaskDto taskDto : successTasks) {
                try {

                    UserDetail user = userDetailMap.get(taskDto.getUserId());
                    if (user == null) {
                        continue;
                    }
                    if (TaskDto.STATUS_RENEWING.equals(taskDto.getStatus())) {
                        taskService.afterRenew(taskDto, user);
                    } else if (TaskDto.STATUS_DELETING.equals(taskDto.getStatus())) {
                        taskService.afterRemove(taskDto, user);
                    }
                } catch (Exception e) {
                    log.info("check reset no response, task id = {}", taskDto.getId().toHexString());
                }
            }

            for (TaskDto taskDto : failedTasks) {
                try {

                    UserDetail user = userDetailMap.get(taskDto.getUserId());
                    if (user == null) {
                        continue;
                    }
                    StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.RENEW_DEL_FAILED, user);
                    if (stateMachineResult.isOk()) {
                        Query query = Query.query(Criteria.where("_id").is(taskDto.getId()));
                        Update update = Update.update("last_updated", new Date());
                        taskService.update(query, update);
                    }
                } catch (Exception e) {
                    log.info("check reset no response, task id = {}", taskDto.getId().toHexString());
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
                    .orOperator(Criteria.where("resetTimes").exists(false), Criteria.where("resetTimes").lt(resetAllTimes));
            Query query = new Query(criteria);
            List<TaskDto> taskDtos = taskService.findAll(query);
            if (CollectionUtils.isEmpty(taskDtos)) {
                return;
            }

            List<String> userLists = taskDtos.stream().map(BaseDto::getUserId).collect(Collectors.toList());

            Map<String, UserDetail> userDetailMap = userService.getUserMapByIdList(userLists);

            for (TaskDto taskDto : taskDtos) {
                try {
                    UserDetail user = userDetailMap.get(taskDto.getUserId());
                    if (user == null) {
                        continue;
                    }
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
