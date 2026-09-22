package com.tapdata.tm.dql.schedule;

import com.tapdata.tm.dql.service.DqlRecoveryBatchService;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class DqlRecoveryBatchTimeoutSchedulerTest {

    @Test
    void scheduledScanDelegatesToRecoveryService() {
        DqlRecoveryBatchService recoveryBatchService = mock(DqlRecoveryBatchService.class);
        DqlRecoveryBatchTimeoutScheduler scheduler = new DqlRecoveryBatchTimeoutScheduler(recoveryBatchService);

        scheduler.scan();

        verify(recoveryBatchService).timeoutExpiredBatches(any(Date.class));
    }
}
