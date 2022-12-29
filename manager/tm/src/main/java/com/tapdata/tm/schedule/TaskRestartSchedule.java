package com.tapdata.tm.schedule;

import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.service.TaskScheduleService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.FunctionUtils;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.commons.collections.CollectionUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @Author: Zed
 * @Date: 2022/2/9
 * @Description:
 */
@Component
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TaskRestartSchedule {

    private TaskService taskService;
    private TaskScheduleService taskScheduleService;
    private UserService userService;
    private SettingsService settingsService;
    private WorkerService workerService;
    private MonitoringLogsService monitoringLogsService;
    private StateMachineService stateMachineService;

    /**
     * 定时重启任务，只要找到有重启标记，并且是停止状态的任务，就重启，每分钟启动一次
     */
    @Scheduled(fixedDelay = 60 * 1000)
    @SchedulerLock(name ="restartTask_lock", lockAtMostFor = "5s", lockAtLeastFor = "5s")
    public void restartTask() {
        Thread.currentThread().setName("taskSchedule-restartTask");
        //查询到所有需要重启的任务
        Criteria criteria = Criteria.where("restartFlag").is(true).and("status").is(TaskDto.STATUS_STOP);
        Query query = new Query(criteria);
        query.fields().include("_id", "restartUserId");
        List<TaskDto> restartTasks = taskService.findAll(query);
        for (TaskDto task : restartTasks) {

            try {
                UserDetail user = userService.loadUserById(MongoUtils.toObjectId(task.getRestartUserId()));
                taskService.start(task.getId(), user);
            } catch (Exception e) {
                log.warn("restart subtask error, task id = {}, e = {}", task.getId(), e.getMessage());
            }
        }
    }

    @Scheduled(initialDelay = 10 * 1000, fixedDelay = 5000)
    @SchedulerLock(name ="engineRestartNeedStartTask_lock", lockAtMostFor = "5s", lockAtLeastFor = "5s")
    public void engineRestartNeedStartTask() {
        Thread.currentThread().setName("taskSchedule-engineRestartNeedStartTask");
        //云版不需要这个重新调度的逻辑
        Object buildProfile = settingsService.getValueByCategoryAndKey(CategoryEnum.SYSTEM, KeyEnum.BUILD_PROFILE);
        if (Objects.isNull(buildProfile)) {
            buildProfile = "DAAS";
        }
        boolean isCloud = buildProfile.equals("CLOUD") || buildProfile.equals("DRS") || buildProfile.equals("DFS");
        long heartExpire = getHeartExpire();

        Criteria criteria = Criteria.where("status").is(TaskDto.STATUS_RUNNING)
                .and("pingTime").lt(System.currentTimeMillis() - heartExpire);
        List<TaskDto> all = taskService.findAll(new Query(criteria));

        if (CollectionUtils.isEmpty(all)) {
            return;
        } else {
            log.info("engineRestartNeedStartTask task size {}", all.size());
        }

        Map<String, UserDetail> userDetailMap = getUserDetailMap(all);
        Map<String, List<Worker>> userWorkerMap = this.getUserWorkMap();
        for (TaskDto taskDto : all) {
            if (isCloud) {
                String status = workerService.checkUsedAgent(taskDto.getAgentId(), userDetailMap.get(taskDto.getUserId()));
                if ("offline".equals(status) || "online".equals(status)) {
                    log.debug("The cloud version does not need this rescheduling");
                    return;
                }
            }
            UserDetail user = userDetailMap.get(taskDto.getUserId());
            if (user == null) {
                continue;
            }

            List<Worker> workerList = userWorkerMap.get(user.getUserId());
            if (CollectionUtils.isEmpty(workerList)) {
                continue;
            }

            StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.OVERTIME, user);
            if (stateMachineResult.isOk()) {
                taskScheduleService.scheduling(taskDto, user);
            }
        }
    }

    @NotNull
    private Map<String, UserDetail> getUserDetailMap(List<TaskDto> all) {
        List<String> userIds = all.stream().map(TaskDto::getUserId).distinct().collect(Collectors.toList());
        List<UserDetail> userByIdList = userService.getUserByIdList(userIds);
        return userByIdList.stream().collect(Collectors.toMap(UserDetail::getUserId, Function.identity(), (e1, e2) -> e1));
    }

    private long getHeartExpire() {
        long heartExpire;
        Settings settings = settingsService.getByCategoryAndKey(CategoryEnum.JOB, KeyEnum.JOB_HEART_TIMEOUT);
        if (Objects.nonNull(settings) && Objects.nonNull(settings.getValue())) {
            heartExpire = Long.parseLong(settings.getValue().toString());
        } else {
            heartExpire = 300000L;
        }
        return heartExpire;
    }



    /**
     * 对于少量因为tm线程终止导致的任务一直启动中（页面的直观显示），也就是调度中状态。做一些定时任务补救措施
     */
    @Scheduled(fixedDelay = 30 * 1000)
    @SchedulerLock(name ="schedulingTask_lock", lockAtMostFor = "10s", lockAtLeastFor = "10s")
    public void overTimeTask() {
        Thread.currentThread().setName("taskSchedule-schedulingTask");
        FunctionUtils.ignoreAnyError(this::stoppingTask);
        FunctionUtils.ignoreAnyError(this::schedulingTask);
        FunctionUtils.ignoreAnyError(this::waitRunTask);
    }

    public void stoppingTask() {
        long heartExpire = getHeartExpire();
        Criteria criteria = Criteria.where("status").is(TaskDto.STATUS_STOPPING)
                .and("stopRetryTimes").lt(8)
                .and("last_updated").lt(new Date(System.currentTimeMillis() - heartExpire));
        List<TaskDto> all = taskService.findAll(Query.query(criteria));

        if (CollectionUtils.isEmpty(all)) {
            return;
        }

        List<String> userList = all.stream().map(BaseDto::getUserId).collect(Collectors.toList());
        Map<String, UserDetail> userMap = userService.getUserMapByIdList(userList);

        for (TaskDto taskDto : all) {
            int stopRetryTimes = taskDto.getStopRetryTimes();
            UserDetail userDetail = userMap.get(taskDto.getUserId());
            if (Objects.isNull(userDetail)) {
                continue;
            }

            if (stopRetryTimes >= 7) {
                CompletableFuture.runAsync(() -> {
                    String template = "The task is being stopped, the number of retries is {0}, it is recommended to try to force stop.";
                    String msg = MessageFormat.format(template, taskDto.getStopRetryTimes());
                    monitoringLogsService.startTaskErrorLog(taskDto, userDetail, msg, Level.WARN);
                });
            } else {
                taskService.sendStoppingMsg(taskDto.getId().toHexString(), taskDto.getAgentId(), userDetail, false);
                Update update = Update.update("stopRetryTimes", taskDto.getStopRetryTimes() + 1).set("last_updated", taskDto.getLastUpdAt());
                taskService.updateById(taskDto.getId(), update, userDetail);
            }
        }
    }

    public void schedulingTask() {
        long heartExpire = getHeartExpire();
        Criteria criteria = Criteria.where("status").is(TaskDto.STATUS_SCHEDULING)
                .and("schedulingTime").lt(System.currentTimeMillis() - heartExpire);
        List<TaskDto> all = taskService.findAll(Query.query(criteria));

        if (CollectionUtils.isEmpty(all)) {
            return;
        }

        Map<String, UserDetail> userMap = this.getUserDetailMap(all);
        Map<String, List<Worker>> userWorkMap = this.getUserWorkMap();

        for (TaskDto taskDto : all) {
            UserDetail user = userMap.get(taskDto.getUserId());
            if (Objects.isNull(user)) {
                continue;
            }

            if (CollectionUtils.isEmpty(userWorkMap.get(user.getUserId()))) {
                continue;
            }

            CompletableFuture.runAsync(() -> {
                String template = "The engine[{0}] takes over the task with a timeout of {1}ms.";
                String msg = MessageFormat.format(template, taskDto.getAgentId(), heartExpire);
                monitoringLogsService.startTaskErrorLog(taskDto, user, msg, Level.WARN);
            });
            StateMachineResult result = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.OVERTIME, user);
            if (result.isOk()) {
                try {
                    CompletableFuture.runAsync(() -> {
                        String template = "In the process of rescheduling tasks, the scheduling engine is {0}.";
                        String msg = MessageFormat.format(template, taskDto.getAgentId());
                        monitoringLogsService.startTaskErrorLog(taskDto, user, msg, Level.WARN);
                    });

                    taskScheduleService.scheduling(taskDto, user);
                } catch (Exception e) {
                    monitoringLogsService.startTaskErrorLog(taskDto, user, e, Level.ERROR);
                    throw e;
                }
            }
        }
    }

    public void waitRunTask() {
        long heartExpire = getHeartExpire();
        Criteria criteria = Criteria.where("status").is(TaskDto.STATUS_WAIT_RUN)
                .and("scheduledTime").lt(System.currentTimeMillis() - heartExpire);
        List<TaskDto> all = taskService.findAll(Query.query(criteria));

        if (CollectionUtils.isEmpty(all)) {
            return;
        }

        Map<String, UserDetail> userMap = this.getUserDetailMap(all);
        Map<String, List<Worker>> userWorkMap = this.getUserWorkMap();
        for (TaskDto taskDto : all) {
            UserDetail user = userMap.get(taskDto.getUserId());
            if (Objects.isNull(user)) {
                continue;
            }
            if (CollectionUtils.isEmpty(userWorkMap.get(user.getUserId()))) {
                continue;
            }

            CompletableFuture.runAsync(() -> {
                String template = "The engine[{0}] takes over the task with a timeout of {1}ms.";
                String msg = MessageFormat.format(template, taskDto.getAgentId(), heartExpire);
                monitoringLogsService.startTaskErrorLog(taskDto, user, msg, Level.WARN);
            });
            StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.OVERTIME, user);
            if (stateMachineResult.isOk()) {
                try {
                    CompletableFuture.runAsync(() -> {
                        String template = "In the process of rescheduling tasks, the scheduling engine is {0}.";
                        String msg = MessageFormat.format(template, taskDto.getAgentId());
                        monitoringLogsService.startTaskErrorLog(taskDto, user, msg, Level.WARN);
                    });

                    taskScheduleService.scheduling(taskDto, user);
                } catch (Exception e) {
                    monitoringLogsService.startTaskErrorLog(taskDto, user, e, Level.ERROR);
                    throw e;
                }
            }
        }
    }

    private Map<String, List<Worker>> getUserWorkMap() {
        List<Worker> workers = workerService.findAvailableAgentBySystem(Collections.emptyList());
        AtomicReference<Map<String, List<Worker>>> userWorkerMap = new AtomicReference<>();
        Optional.ofNullable(workers).ifPresent(list -> userWorkerMap.set(list.stream().collect(Collectors.groupingBy(Worker::getUserId))));

        return userWorkerMap.get();
    }
}
