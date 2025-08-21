package com.tapdata.tm.schedule;


import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.clusterOperation.service.ClusterOperationService;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 定时执行，升级agent版本
 */
@Slf4j
@Component
@Setter(onMethod_ = {@Autowired})
public class AgentUpdateSchedule {
    private ClusterOperationService clusterOperationService;
    private WorkerService workerService;
    private SettingsService settingsService;

    /**
     * @desc 执行扫描，每120S执行一次
     */
    @Scheduled(fixedDelay = 120 * 1000)
    @SchedulerLock(name = "AgentUpdateSchedule", lockAtMostFor = "120s", lockAtLeastFor = "60s")
    public void execute() {
			Thread.currentThread().setName(getClass().getSimpleName() + "-execute");
        log.debug("清理 clusterOperation");
        clusterOperationService.cleanOperation();

        log.debug("清理 cleanWorkers");
//        if(server && server.daasSettings && server.daasSettings['buildProfile'] !== 'DAAS')
        String buildProfile = String.valueOf(settingsService.getByCategoryAndKey("System", "buildProfile"));
        if (!"DAAS".equals(buildProfile)){
            workerService.cleanWorkers();
        }

        log.debug("执行扫描，更新agent 版本");
        clusterOperationService.sendOperation();
    }

}
