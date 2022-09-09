package com.tapdata.tm.schedule;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * @author jiuyetx
 * @date 2022/9/9
 */
@Component
public class AlarmSchedule {
    @Scheduled(cron = "0 */1 * * * ?")
    public void schedule() {

    }
}
