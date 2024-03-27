package com.tapdata.tm.schedule;

import com.tapdata.tm.worker.service.WorkerService;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class WorkerExpireScheduler {

    private WorkerService workerService;

    /**
     * 每9分钟运行一次
     */
    @Scheduled(cron = "0 0/9 * * * ?")
    @SchedulerLock(name = "checkWorkerExpire", lockAtMostFor = "PT5M", lockAtLeastFor = "PT5M")
    public void checkWorkerExpire() {
			Thread.currentThread().setName(getClass().getSimpleName() + "-checkWorkerExpire");
        workerService.checkWorkerExpire();
    }
}
