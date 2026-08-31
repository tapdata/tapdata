package com.tapdata.tm.dql.schedule;

import com.tapdata.tm.dql.service.DqlRecoveryBatchService;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class DqlEventStatusScheduler {
    private final DqlRecoveryBatchService recoveryBatchService;

    public DqlEventStatusScheduler(DqlRecoveryBatchService recoveryBatchService) {
        this.recoveryBatchService = recoveryBatchService;
    }

    @Scheduled(fixedDelay = 30_000L, initialDelay = 30_000L)
    @SchedulerLock(name = "dql_event_status_sync", lockAtMostFor = "PT1M", lockAtLeastFor = "PT5S")
    public void scan() {
        int changed = recoveryBatchService.synchronizeNotReprocessableEvents();
        if (changed > 0) {
            log.info("DQL recovery status synchronization changed {} events", changed);
        }
    }
}
