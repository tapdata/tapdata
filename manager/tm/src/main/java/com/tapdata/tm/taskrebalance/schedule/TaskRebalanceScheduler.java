package com.tapdata.tm.taskrebalance.schedule;

import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.dblock.DBLock;
import com.tapdata.tm.dblock.DBLockConfiguration;
import com.tapdata.tm.dblock.DBLockRepository;
import com.tapdata.tm.dblock.ILock;
import com.tapdata.tm.dblock.impl.ActiveServiceLock;
import com.tapdata.tm.taskrebalance.service.TaskRebalanceService;
import jakarta.annotation.PreDestroy;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Component
public class TaskRebalanceScheduler implements SmartInitializingSingleton {
    private static final String LOCK_KEY = "TaskRebalanceScheduler.schedule";
    private static final long SCHEDULE_DELAY_SECONDS = 5L;

    private final TaskRebalanceService taskRebalanceService;
    private final SettingsService settingsService;
    private final DBLockRepository dbLockRepository;
    private final DBLockConfiguration dbLockConfiguration;
    private final ScheduledExecutorService scheduleExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "TaskRebalanceScheduler-schedule");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicReference<ScheduledFuture<?>> scheduleFuture = new AtomicReference<>();
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private volatile ILock lock;
    private volatile ActiveServiceLock activeServiceLock;

    public TaskRebalanceScheduler(@NonNull TaskRebalanceService taskRebalanceService,
                                  @NonNull SettingsService settingsService,
                                  @NonNull DBLockRepository dbLockRepository,
                                  @NonNull DBLockConfiguration dbLockConfiguration) {
        this.taskRebalanceService = taskRebalanceService;
        this.settingsService = settingsService;
        this.dbLockRepository = dbLockRepository;
        this.dbLockConfiguration = dbLockConfiguration;
    }

    @Override
    public void afterSingletonsInstantiated() {
        if (settingsService.isCloud()) {
            log.info("TaskRebalanceScheduler is disabled in cloud mode");
            return;
        }
        log.info("TaskRebalanceScheduler initializing, lockKey={}, owner={}", LOCK_KEY, dbLockConfiguration.getOwner());
        this.lock = DBLock.create(dbLockRepository, LOCK_KEY);
        this.initialized.set(true);
        this.activeServiceLock = lock.activeService(
                dbLockConfiguration.getOwner(),
                dbLockConfiguration.getExpireSeconds(),
                dbLockConfiguration.getHeartbeatSeconds(),
                activeServiceLock -> startSchedule(),
                activeServiceLock -> stopSchedule()
        );
    }

    private void startSchedule() {
        if (!initialized.get()) {
            log.warn("TaskRebalanceScheduler DBLock is not initialized");
            return;
        }
        log.info("TaskRebalanceScheduler starting schedule, delaySeconds={}", SCHEDULE_DELAY_SECONDS);
        ScheduledFuture<?> future = scheduleExecutor.scheduleWithFixedDelay(
                this::scheduleSafely,
                0L,
                SCHEDULE_DELAY_SECONDS,
                TimeUnit.SECONDS
        );
        if (!scheduleFuture.compareAndSet(null, future)) {
            future.cancel(true);
            log.info("TaskRebalanceScheduler schedule is already running");
            return;
        }
        log.info("TaskRebalanceScheduler schedule started");
    }

    private void stopSchedule() {
        ScheduledFuture<?> future = scheduleFuture.getAndSet(null);
        if (future != null) {
            future.cancel(true);
            log.info("TaskRebalanceScheduler schedule stopped");
        }
    }

    private void scheduleSafely() {
        try {
            taskRebalanceService.scheduleOnce();
        } catch (Exception e) {
            log.warn("TaskRebalanceScheduler schedule failed", e);
        }
    }

    @PreDestroy
    public void destroy() {
        log.info("TaskRebalanceScheduler destroying");
        stopSchedule();
        scheduleExecutor.shutdownNow();
        if (activeServiceLock != null) {
            try {
                activeServiceLock.close();
            } catch (Exception e) {
                log.warn("TaskRebalanceScheduler close active service lock failed", e);
            }
        }
    }
}
