package com.tapdata.tm.schedule;

import com.tapdata.tm.events.service.EventsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 定时检查是否又发送失败的邮件或者短信，有就重新发送ss
 */
@Slf4j
@Component
public class InformUserSchedule {
    @Autowired
    EventsService eventsService;

    /**
     * @desc 执行扫描，每 3分钟执行一次
     */
    //@Scheduled(cron = "0 */3 * * * ?")
//    @SchedulerLock(name ="InformUserSchedule.execute", lockAtMostFor = "5m", lockAtLeastFor = "5m")
    public void execute() {
        log.info("send notified message");
        eventsService.completeInform();
    }
}
