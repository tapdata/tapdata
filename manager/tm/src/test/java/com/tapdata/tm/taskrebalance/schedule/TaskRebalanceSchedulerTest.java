package com.tapdata.tm.taskrebalance.schedule;

import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.dblock.DBLockConfiguration;
import com.tapdata.tm.dblock.DBLockRepository;
import com.tapdata.tm.dblock.impl.ActiveServiceLock;
import com.tapdata.tm.taskrebalance.service.TaskRebalanceService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class TaskRebalanceSchedulerTest {

    @Test
    @DisplayName("cloud mode skips scheduler initialization")
    void afterSingletonsInstantiatedSkipsCloudMode() {
        SettingsService settingsService = mock(SettingsService.class);
        DBLockRepository dbLockRepository = mock(DBLockRepository.class);
        DBLockConfiguration dbLockConfiguration = mock(DBLockConfiguration.class);
        TaskRebalanceScheduler scheduler = new TaskRebalanceScheduler(
                mock(TaskRebalanceService.class),
                settingsService,
                dbLockRepository,
                dbLockConfiguration
        );
        when(settingsService.isCloud()).thenReturn(true);

        scheduler.afterSingletonsInstantiated();
        scheduler.destroy();

        verify(settingsService).isCloud();
        verifyNoInteractions(dbLockRepository, dbLockConfiguration);
    }

    @Test
    @DisplayName("scheduleSafely marks owner before scheduling and swallows service exceptions")
    void scheduleSafelyMarksOwnerAndSwallowsException() {
        TaskRebalanceService taskRebalanceService = mock(TaskRebalanceService.class);
        TaskRebalanceScheduler scheduler = newScheduler(taskRebalanceService);
        doThrow(new RuntimeException("boom")).when(taskRebalanceService).scheduleOnce();

        ReflectionTestUtils.invokeMethod(scheduler, "scheduleSafely");
        scheduler.destroy();

        verify(taskRebalanceService).markRunningRebalancesOwner();
        verify(taskRebalanceService).scheduleOnce();
    }

    @Test
    @DisplayName("destroy closes active service lock")
    void destroyClosesActiveServiceLock() throws Exception {
        TaskRebalanceScheduler scheduler = newScheduler(mock(TaskRebalanceService.class));
        ActiveServiceLock activeServiceLock = mock(ActiveServiceLock.class);
        ReflectionTestUtils.setField(scheduler, "activeServiceLock", new AtomicReference<>(activeServiceLock));

        scheduler.destroy();

        verify(activeServiceLock).close();
    }

    private TaskRebalanceScheduler newScheduler(TaskRebalanceService taskRebalanceService) {
        return new TaskRebalanceScheduler(
                taskRebalanceService,
                mock(SettingsService.class),
                mock(DBLockRepository.class),
                mock(DBLockConfiguration.class)
        );
    }
}
