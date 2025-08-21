package com.tapdata.tm.schedule;

import com.tapdata.tm.task.service.TaskOperationRateLimitService;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 任务操作限流记录清理调度器
 * 定期清理过期的限流记录，避免内存泄漏
 * 
 * @author tapdata
 */
@Component
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TaskOperationRateLimitCleanupSchedule {
    
    private TaskOperationRateLimitService taskOperationRateLimitService;
    
    /**
     * 每小时清理一次过期的限流记录
     */
    @Scheduled(fixedDelay = 60 * 60 * 1000)
    @SchedulerLock(name = "taskOperationRateLimitCleanup", lockAtMostFor = "30m", lockAtLeastFor = "5m")
    public void cleanupExpiredRecords() {
        Thread.currentThread().setName(getClass().getSimpleName() + "-cleanupExpiredRecords");
        
        try {
            taskOperationRateLimitService.cleanupExpiredRecords();
            log.debug("Task operation rate limit cleanup completed");
        } catch (Exception e) {
            log.error("Failed to cleanup expired task operation rate limit records", e);
        }
    }
}
