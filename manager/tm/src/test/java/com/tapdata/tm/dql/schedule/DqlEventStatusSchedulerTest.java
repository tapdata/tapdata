package com.tapdata.tm.dql.schedule;

import com.tapdata.tm.dql.service.DqlRecoveryBatchService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DqlEventStatusSchedulerTest {

    @Test
    void scheduledScanDelegatesToRecoveryService() {
        DqlRecoveryBatchService recoveryBatchService = mock(DqlRecoveryBatchService.class);
        when(recoveryBatchService.synchronizeNotReprocessableEvents()).thenReturn(0);
        DqlEventStatusScheduler scheduler = new DqlEventStatusScheduler(recoveryBatchService);

        scheduler.scan();

        verify(recoveryBatchService).synchronizeNotReprocessableEvents();
    }
}
