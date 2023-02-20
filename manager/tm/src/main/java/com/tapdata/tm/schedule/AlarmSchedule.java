package com.tapdata.tm.schedule;

import com.tapdata.tm.alarm.service.AlarmService;
import lombok.Setter;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * @author jiuyetx
 * @date 2022/9/9
 */
@Component
@Setter(onMethod_ = {@Autowired})
public class AlarmSchedule {
    private AlarmService alarmService;

    @Scheduled(cron = "0/30 * * * * ?")
    @SchedulerLock(name ="task_notify_alarm", lockAtMostFor = "10s", lockAtLeastFor = "10s")
    public void schedule() {
        alarmService.notifyAlarm();
    }
}
