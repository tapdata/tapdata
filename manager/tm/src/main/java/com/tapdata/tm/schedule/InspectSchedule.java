package com.tapdata.tm.schedule;

import com.tapdata.tm.dblock.DBLock;
import com.tapdata.tm.dblock.DBLockConfiguration;
import com.tapdata.tm.dblock.impl.ActiveServiceLock;
import com.tapdata.tm.dblock.repository.MongoDBLockRepository;
import com.tapdata.tm.inspect.InspectJobSchedule;
import com.tapdata.tm.inspect.service.InspectService;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 *
 */
@Slf4j
@Component
public class InspectSchedule implements DisposableBean {
    private static final String TAG = InspectSchedule.class.getSimpleName();

    @Autowired
    private InspectService inspectService;
    @Autowired
    private InspectJobSchedule jobSchedule;
    @Autowired
    private MongoDBLockRepository repository;
    @Autowired
    private DBLockConfiguration lockConfig;

    private ActiveServiceLock lock;
    private CompletableFuture<Void> loopFuture;

    @PostConstruct
    public void init() throws Exception {
        this.lock = DBLock.create(repository, TAG).activeService(
            lockConfig.getOwner()
            , lockConfig.getExpireSeconds()
            , lockConfig.getHeartbeatSeconds()
            , this::doActive
            , this::doStandby
        );
    }

    @Override
    public void destroy() throws Exception {
        if (null != lock) {
            lock.close();
        }
    }

    protected void doActive(ActiveServiceLock lock) {
        log.info(DBLock.prefixTag(" active for '%s'", lock.getKey()));
        loopFuture = CompletableFuture.runAsync(() -> {
            String threadName = Thread.currentThread().getName();
            Thread.currentThread().setName(String.format("%s-%s", threadName, TAG));

            while (lock.isActive()) {
                try {
                    inspectService.cleanDeadInspect();
                } catch (Exception e) {
                    log.warn(DBLock.prefixTag(" error while clean dead inspect"), e);
                }

                try {
                    inspectService.setRepeatInspectTask();
                } catch (Exception e) {
                    log.warn(DBLock.prefixTag(" error while set repeat inspect task"), e);
                }

                try {
                    TimeUnit.MINUTES.sleep(1);
                } catch (InterruptedException e) {
                    log.error(DBLock.prefixTag(" interrupted while sleeping"), e);
                    break;
                }
            }
        }, DBLock.executor);
    }

    protected void doStandby(ActiveServiceLock lock) throws Exception {
        log.info(DBLock.prefixTag(" standby for '%s'", lock.getKey()));
        loopFuture.cancel(true);
        loopFuture = null;
        jobSchedule.cleanAllJobs();
    }
}
