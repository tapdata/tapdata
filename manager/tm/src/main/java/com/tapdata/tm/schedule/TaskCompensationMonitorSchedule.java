package com.tapdata.tm.schedule;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 任务补偿轮询监控调度器
 * 定期输出补偿轮询的状态信息
 * 
 * @author tapdata
 */
@Slf4j
@Component
public class TaskCompensationMonitorSchedule {
    
    @Autowired
    private TaskStartCompensationSchedule taskStartCompensationSchedule;
    
    /**
     * 监控任务补偿轮询状态
     * 每60秒输出一次状态信息
     */
    @Scheduled(fixedDelay = 60000L)
    public void monitorCompensationStatus() {
        try {
            String status = taskStartCompensationSchedule.getPollingStatus();
            log.info("Task compensation polling status: {}", status);
            
        } catch (Exception e) {
            log.error("Error monitoring task compensation status", e);
        }
    }
}
