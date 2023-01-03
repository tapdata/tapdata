package com.tapdata.tm.schedule;

import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.FunctionUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class MigrateDagSchedule {

    private TaskService taskService;

    @Scheduled(fixedDelay = 10 * 1000)
    @SchedulerLock(name = "migrateDagPlanStartLock", lockAtMostFor = "5s", lockAtLeastFor = "5s")
    public void migrateDagPlanStart() {
        FunctionUtils.ignoreAnyError(() -> taskService.startPlanMigrateDagTask());
        FunctionUtils.ignoreAnyError(() -> taskService.startPlanCronTask());
    }
}
