package com.tapdata.tm.schedule;

import com.tapdata.tm.task.service.TaskStartQueueService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 任务启动队列处理调度器
 * 定时处理队列中等待启动的任务
 * 
 * @author tapdata
 */
@Slf4j
@Component
public class TaskStartQueueSchedule {
    
    @Autowired
    private TaskStartQueueService taskStartQueueService;
    
    /**
     * 处理任务启动队列
     * 每1秒检查一次队列，尝试启动下一个任务
     */
    @Scheduled(fixedDelay = 1000L)
    public void processTaskStartQueue() {
        try {
            // 持续处理队列，直到没有任务可以启动
            int processedCount = 0;
            while (taskStartQueueService.processNextTaskStart()) {
                processedCount++;
                // 限制单次处理的任务数量，避免长时间占用
                if (processedCount >= 10) {
                    log.debug("Processed {} tasks in this cycle, will continue in next cycle", processedCount);
                    break;
                }
            }
            
            if (processedCount > 0) {
                log.debug("Processed {} queued task start requests", processedCount);
            }
            
        } catch (Exception e) {
            log.error("Error processing task start queue", e);
        }
    }
    
    /**
     * 监控任务启动队列状态
     * 每30秒输出一次队列状态
     */
    @Scheduled(fixedDelay = 30000L)
    public void monitorTaskStartQueue() {
        try {
            int queueSize = taskStartQueueService.getQueueSize();
            int remainingCapacity = taskStartQueueService.getRemainingCapacity();
            
            if (queueSize > 0) {
                log.info("Task start queue status: queueSize={}, remainingCapacity={}", queueSize, remainingCapacity);
            }
            
            // 队列积压警告
            if (queueSize > 100) {
                log.warn("Task start queue size is high: {}, may indicate rate limiting bottleneck", queueSize);
            }
            
            if (queueSize > 800) {
                log.error("Task start queue size is critically high: {}, system may be overloaded", queueSize);
            }
            
        } catch (Exception e) {
            log.error("Error monitoring task start queue", e);
        }
    }
}
