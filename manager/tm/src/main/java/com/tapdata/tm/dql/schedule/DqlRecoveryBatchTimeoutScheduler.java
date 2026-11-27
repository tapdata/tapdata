package com.tapdata.tm.dql.schedule;

import com.tapdata.tm.dql.service.DqlRecoveryBatchService;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class DqlRecoveryBatchTimeoutScheduler {
    private final DqlRecoveryBatchService recoveryBatchService;

    public DqlRecoveryBatchTimeoutScheduler(DqlRecoveryBatchService recoveryBatchService) {
        this.recoveryBatchService = recoveryBatchService;
    }

    @Scheduled(fixedDelay = 30_000L, initialDelay = 30_000L)
    @SchedulerLock(name = "dql_recovery_batch_timeout", lockAtMostFor = "PT1M", lockAtLeastFor = "PT5S")
    public void scan() {
        recoveryBatchService.timeoutExpiredBatches(new Date());
    }
}
