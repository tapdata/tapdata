package com.tapdata.tm.schedule;

import com.tapdata.tm.inspect.service.InspectService;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

/**
 */
@Slf4j
@Component
public class InspectSchedule {


    @Autowired
    InspectService inspectService;


    /**
     * @desc 执行扫描，每 1分钟执行一次
     */
//    @Scheduled(cron = "2 0/1 * * * ?")
//    @Scheduled(cron = "0 */5 * * * ?")
    @Scheduled(cron = "2 0/1 * * * ?")
    @SchedulerLock(name = "InspectSchedule.execute", lockAtMostFor = "1m", lockAtLeastFor = "1m")
    public void execute() {
			Thread.currentThread().setName(getClass().getSimpleName() + "-execute");
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        String processName = runtimeMXBean.getName();
        log.info(processName+"**************** 获得了 InspectSchedule  锁");
        inspectService.setRepeatInspectTask();

    }

    /**
     * @desc 执行扫描，每 1分钟执行一次
     */
    @Scheduled(cron = "2 0/1 * * * ?")
    @SchedulerLock(name = "InspectSchedule.execute", lockAtMostFor = "1m", lockAtLeastFor = "1m")
    public void cleanDeadInspect() {
			Thread.currentThread().setName(getClass().getSimpleName() + "-cleanDeadInspect");
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        String processName = runtimeMXBean.getName();
        log.info(processName+"**************** 获得了 cleanDeadInspect  锁");
        inspectService.cleanDeadInspect();

    }

}
