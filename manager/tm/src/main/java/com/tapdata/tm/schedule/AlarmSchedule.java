package com.tapdata.tm.schedule;

import com.tapdata.tm.alarm.service.AlarmNotifyService;
import com.tapdata.tm.alarm.service.ApiServerAlarmConfig;
import com.tapdata.tm.alarm.service.ApiServerAlarmService;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author jiuyetx
 * @date 2022/9/9
 */
@Slf4j
@Component
@Setter(onMethod_ = {@Autowired})
public class AlarmSchedule implements InitializingBean {
    final AtomicBoolean init = new AtomicBoolean(false);
    private ApiServerAlarmService apiServerAlarmService;
    private ApiServerAlarmConfig apiServerAlarmConfig;
    private AlarmNotifyService alarmNotifyService;


    @Scheduled(cron = "0/30 * * * * ?")
    @SchedulerLock(name ="task_notify_alarm", lockAtMostFor = "10s", lockAtLeastFor = "10s")
    public void schedule() {
			Thread.currentThread().setName(getClass().getSimpleName() + "-schedule");
        alarmNotifyService.notifyAlarm();
    }

    @Scheduled(cron = "0 0/1 * * * ?")
    @SchedulerLock(name ="api_server_notify_alarm", lockAtMostFor = "10s", lockAtLeastFor = "10s")
    public void apiServer() {
        if (!init.get()) {
            afterPropertiesSet();
        }
        Thread.currentThread().setName(getClass().getSimpleName() + "api-server-alarm-schedule");
        apiServerAlarmService.scanMetricData();
    }

    @Scheduled(initialDelay = 10, fixedDelay = 30000)
    public void taskRetryAlarm() {
        Thread.currentThread().setName(getClass().getSimpleName() + "api-server-alarm-config-update-schedule");
        apiServerAlarmConfig.updateConfig();
        init.set(true);
    }

    @Override
    public void afterPropertiesSet() {
        try {
            apiServerAlarmConfig.updateConfig();
            init.set(true);
        } catch (Exception e) {
            log.info(e.getMessage());
        }
    }
}
