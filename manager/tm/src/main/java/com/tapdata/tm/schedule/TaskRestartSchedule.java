package com.tapdata.tm.schedule;

import cn.hutool.core.util.RandomUtil;
import com.tapdata.tm.Settings.constant.CategoryEnum;
import com.tapdata.tm.Settings.constant.KeyEnum;
import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.service.TaskScheduleService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.*;
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

        List<String> userIds = all.stream().map(TaskDto::getUserId).distinct().collect(Collectors.toList());
        List<UserDetail> userByIdList = userService.getUserByIdList(userIds);
        Map<String, UserDetail> userDetailMap = userByIdList.stream().collect(Collectors.toMap(UserDetail::getUserId, Function.identity(), (e1, e2) -> e1));

        all.forEach(taskDto -> {
            if (isCloud) {
                String status = workerService.checkUsedAgent(taskDto.getAgentId(), userDetailMap.get(taskDto.getUserId()));
                if ("offline".equals(status) || "online".equals(status)) {
                    log.debug("The cloud version does not need this rescheduling");
                    return;
                }
            }
            UserDetail user = userDetailMap.get(taskDto.getUserId());
            if (user == null) {
                return;
            }

            StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.OVERTIME, user);
            if (stateMachineResult.isOk()) {
                taskScheduleService.scheduling(taskDto, user);
            }
        });
    }

    private long getHeartExpire() {
        long heartExpire;
        Settings settings = settingsService.getByCategoryAndKey(CategoryEnum.JOB, KeyEnum.JOB_HEART_TIMEOUT);
        if (Objects.nonNull(settings) && Objects.nonNull(settings.getValue())) {
            heartExpire = Long.parseLong(settings.getValue().toString());
        } else {
            heartExpire = 300000;
        }
        return heartExpire;
    }



    /**
     * 对于少量因为tm线程终止导致的任务一直启动中（页面的直观显示），也就是调度中状态。做一些定时任务补救措施
     */
    @Scheduled(fixedDelay = 30 * 1000)
    @SchedulerLock(name ="schedulingTask_lock", lockAtMostFor = "10s", lockAtLeastFor = "10s")
    public void orverTimeTask() {
        Thread.currentThread().setName("taskSchedule-schedulingTask");
        try {
            stoppingTask();
        } catch (Exception e) {
            log.warn("stopping overtime task retry failed");
        }

        try {
            schedulingTask();
        } catch (Exception e) {
            log.warn("scheduling overtime task retry failed");
        }

        try {
            waitRunTask();
        } catch (Exception e) {
            log.warn("wait run overtime task retry failed");
        }
    }

    public void stoppingTask() {
        long heartExpire = getHeartExpire();

        Criteria criteria = Criteria.where("status").is(TaskDto.STATUS_STOPPING)
                .and("scheduledTime").lt(new Date(System.currentTimeMillis() - heartExpire));
        List<TaskDto> all = taskService.findAll(new Query(criteria));

        if (CollectionUtils.isEmpty(all)) {
            return;
        }

        List<String> userList = all.stream().map(BaseDto::getUserId).collect(Collectors.toList());
        Map<String, UserDetail> userMap = userService.getUserMapByIdList(userList);


        all.forEach(taskDto -> {
            stateMachineService.executeAboutTask(taskDto, DataFlowEvent.OVERTIME, userMap.get(taskDto.getUserId()));
        });
    }


    public void schedulingTask() {
        long heartExpire = getHeartExpire();

        Criteria criteria = Criteria.where("status").is(TaskDto.STATUS_SCHEDULING)
                .and("last_updated").lt(new Date(System.currentTimeMillis() - heartExpire));
        List<TaskDto> all = taskService.findAll(new Query(criteria));

        if (CollectionUtils.isEmpty(all)) {
            return;
        }

        List<String> userList = all.stream().map(BaseDto::getUserId).collect(Collectors.toList());
        Map<String, UserDetail> userMap = userService.getUserMapByIdList(userList);

        Iterator<TaskDto> iterator = all.iterator();

        Long now = System.currentTimeMillis();
        while (iterator.hasNext()) {
            TaskDto next = iterator.next();
            UserDetail user = userMap.get(next.getUserId());
            if (user != null) {
                if (Objects.nonNull(next.getScheduleDate()) && (now - next.getScheduleDate() > heartExpire)) {
                    stateMachineService.executeAboutTask(next, DataFlowEvent.OVERTIME, user);
                    iterator.remove();
                }
            }
        }


        all.forEach(taskDto -> {
            taskScheduleService.scheduling(taskDto, userMap.get(taskDto.getUserId()));
        });
    }


    public void waitRunTask() {
        long heartExpire = getHeartExpire();

        Criteria criteria = Criteria.where("status").is(TaskDto.STATUS_WAIT_RUN)
                .and("scheduledTime").lt(new Date(System.currentTimeMillis() - heartExpire));
        List<TaskDto> all = taskService.findAll(new Query(criteria));

        if (CollectionUtils.isEmpty(all)) {
            return;
        }

        List<String> userList = all.stream().map(BaseDto::getUserId).collect(Collectors.toList());
        Map<String, UserDetail> userMap = userService.getUserMapByIdList(userList);
        all.forEach(taskDto -> {
            boolean start = true;
            StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.OVERTIME, userMap.get(taskDto.getUserId()));
            if (stateMachineResult.isFail()) {
                start = false;
            }
            if (start) {
                taskScheduleService.scheduling(taskDto, userMap.get(taskDto.getUserId()));
            }
        });
    }
}
