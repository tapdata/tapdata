package com.tapdata.tm.schedule;

import com.tapdata.tm.Settings.entity.Settings;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    private UserService userService;
    private SettingsService settingsService;
    private WorkerService workerService;

    /**
     * 定时重启任务，只要找到有重启标记，并且是停止状态的任务，就重启，每分钟启动一次
     */
    @Scheduled(fixedDelay = 10 * 1000)
    @SchedulerLock(name ="restart_task_lock", lockAtMostFor = "5s", lockAtLeastFor = "5s")
    public void restartTask() {
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

    @Scheduled(initialDelay = 10 * 1000, fixedDelay = 5 * 60 * 1000)
    @SchedulerLock(name ="restart_task_lock", lockAtMostFor = "5s", lockAtLeastFor = "5s")
    public void engineRestartNeedStartTask() {


        //云版不需要这个重新调度的逻辑
        Object buildProfile = settingsService.getByCategoryAndKey("System", "buildProfile");
        if (Objects.isNull(buildProfile)) {
            buildProfile = "DAAS";
        }
        boolean isCloud = buildProfile.equals("CLOUD") || buildProfile.equals("DRS") || buildProfile.equals("DFS");
        long heartExpire;
        Settings settings = settingsService.findAll().stream().filter(k -> "jobHeartTimeout".equals(k.getKey())).findFirst().orElse(null);
        if (Objects.nonNull(settings) && Objects.nonNull(settings.getValue())) {
            heartExpire = Long.parseLong(settings.getValue().toString());
        } else {
            heartExpire = 300000;
        }

        Criteria criteria = Criteria.where("status").is(TaskDto.STATUS_RUNNING).and("pingTime").lt(System.currentTimeMillis() - heartExpire);
        List<TaskDto> all = taskService.findAll(new Query(criteria));

        if (CollectionUtils.isEmpty(all)) {
            return;
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
            taskService.run(taskDto, userDetailMap.get(taskDto.getUserId()));
        });
    }


    /**
     * 对于少量因为tm线程终止导致的任务一直启动中（页面的直观显示），也就是调度中状态。做一些定时任务补救措施
     */
    @Scheduled(fixedDelay = 30 * 1000)
    @SchedulerLock(name ="restart_task_lock", lockAtMostFor = "10s", lockAtLeastFor = "10s")
    public void schedulingTask() {
        long heartExpire = 60000;

        Criteria criteria = Criteria.where("status").is(TaskDto.STATUS_WAIT_RUN).and("last_updated").lt(new Date(System.currentTimeMillis() - heartExpire));
        List<TaskDto> all = taskService.findAll(new Query(criteria));

        if (CollectionUtils.isEmpty(all)) {
            return;
        }

        List<String> userIds = all.stream().map(TaskDto::getUserId).distinct().collect(Collectors.toList());
        List<UserDetail> userByIdList = userService.getUserByIdList(userIds);
        Map<String, UserDetail> userDetailMap = userByIdList.stream().collect(Collectors.toMap(UserDetail::getUserId, Function.identity(), (e1, e2) -> e1));

        all.forEach(taskDto -> {
            taskService.run(taskDto, userDetailMap.get(taskDto.getUserId()));
        });
    }
}
