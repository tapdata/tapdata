package com.tapdata.tm.schedule;

import com.tapdata.tm.task.service.TaskExtendService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.FunctionUtils;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MigrateDagSchedule {

    private final TaskService taskService;

    public MigrateDagSchedule(TaskService taskService) {
        this.taskService = taskService;
    }


    @Scheduled(fixedDelay = 10 * 1000)
    @SchedulerLock(name = "migrateDagPlanStartLock", lockAtMostFor = "5s", lockAtLeastFor = "5s")
    public void migrateDagPlanStart() {
        taskService.startPlanMigrateDagTask();
    }

    @Scheduled(fixedDelay = 10 * 1000)
    @SchedulerLock(name = "startPlanCronTaskLock", lockAtMostFor = "30s", lockAtLeastFor = "10s")
    public void startPlanCronTask() {
        taskService.startPlanCronTask();
    }


}
