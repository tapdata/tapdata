package com.tapdata.tm.schedule;

import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.task.service.TaskStartQueueService;
import com.tapdata.tm.user.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 任务启动补偿轮询调度器
 * 查询状态为启动中但没有被下发到引擎队列的任务，进行补偿启动
 * 轮询间隔：第一次10秒，如果没有任务则逐步提升，每次增加10秒，最大300秒
 * 
 * @author tapdata
 */
@Slf4j
@Component
public class TaskStartCompensationSchedule {
    
    @Autowired
    private TaskService taskService;
    
    @Autowired
    private TaskStartQueueService taskStartQueueService;
    
    @Autowired
    private UserService userService;
    
    /**
     * 基础轮询间隔：10秒
     */
    private static final long BASE_INTERVAL_MS = 10 * 1000L;
    
    /**
     * 间隔增量：10秒
     */
    private static final long INTERVAL_INCREMENT_MS = 10 * 1000L;
    
    /**
     * 最大轮询间隔：300秒
     */
    private static final long MAX_INTERVAL_MS = 300 * 1000L;
    
    /**
     * 当前轮询间隔倍数
     */
    private final AtomicInteger intervalMultiplier = new AtomicInteger(1);
    
    /**
     * 上次轮询时间
     */
    private final AtomicLong lastPollTime = new AtomicLong(0);
    
    /**
     * 系统用户（用于执行补偿任务）
     */
    private UserDetail systemUser;
    
    /**
     * 任务启动补偿轮询
     * 使用动态间隔调度
     */
    @Scheduled(fixedDelay = 10000L) // 基础10秒间隔
    public void compensateTaskStart() {
        try {
            long currentTime = System.currentTimeMillis();
            long currentInterval = getCurrentInterval();

            // 检查是否到了轮询时间
            if (currentTime - lastPollTime.get() < currentInterval) {
                return;
            }

            lastPollTime.set(currentTime);

            log.debug("Starting task start compensation poll, current interval: {}ms", currentInterval);

            // 检查是否有任务在排队中，如果有则跳过补偿
            int queueSize = taskStartQueueService.getQueueSize();
            if (queueSize > 0) {
                log.debug("Task start queue has {} pending tasks, skipping compensation to avoid duplication", queueSize);
                // 有任务在排队，但不增加间隔，保持当前频率监控
                return;
            }

            // 查询需要补偿的任务
            List<TaskDto> compensationTasks = findCompensationTasks();

            if (compensationTasks.isEmpty()) {
                // 没有任务需要补偿，增加轮询间隔
                increaseInterval();
                log.debug("No tasks need compensation, increased interval to: {}ms", getCurrentInterval());
            } else {
                // 有任务需要补偿，重置轮询间隔
                resetInterval();
                log.info("Found {} tasks need compensation, reset interval to: {}ms",
                        compensationTasks.size(), getCurrentInterval());

                // 处理补偿任务
                processCompensationTasks(compensationTasks);
            }

        } catch (Exception e) {
            log.error("Error in task start compensation poll", e);
        }
    }
    
    /**
     * 查询需要补偿的任务
     * 条件：状态为启动中(SCHEDULING)，且超过一定时间没有被处理
     * 
     * @return 需要补偿的任务列表
     */
    private List<TaskDto> findCompensationTasks() {
        // 查询状态为SCHEDULING的任务
        Criteria criteria = Criteria.where("status").is(TaskDto.STATUS_SCHEDULING);
        
        // 添加时间条件：schedulingTime超过30秒的任务
        long compensationThreshold = System.currentTimeMillis() - 30 * 1000L;
        criteria.and("schedulingTime").lt(compensationThreshold);
        
        Query query = new Query(criteria);
        query.fields().include("_id", "name", "agentId", "schedulingTime", "userId");
        
        List<TaskDto> tasks = taskService.findAll(query);
        
        log.debug("Found {} tasks in SCHEDULING status older than 30 seconds", tasks.size());
        
        return tasks;
    }
    
    /**
     * 处理补偿任务
     * 
     * @param compensationTasks 需要补偿的任务列表
     */
    private void processCompensationTasks(List<TaskDto> compensationTasks) {
        UserDetail user = getSystemUser();
        
        for (TaskDto task : compensationTasks) {
            try {
                String taskId = task.getId().toHexString();
                String agentId = task.getAgentId();
                
                if (agentId == null) {
                    log.warn("Task {} has no agentId, skipping compensation", taskId);
                    continue;
                }
                
                long schedulingTime = task.getSchedulingTime() != null ? task.getSchedulingTime().getTime() : 0;
                long waitingTime = System.currentTimeMillis() - schedulingTime;
                
                log.info("Compensating task start: taskId={}, taskName={}, agentId={}, waitingTime={}ms", 
                        taskId, task.getName(), agentId, waitingTime);
                
                // 使用任务启动队列服务进行补偿启动
                boolean startedImmediately = taskStartQueueService.requestStartTask(taskId, agentId, user, "schedule");
                
                if (startedImmediately) {
                    log.info("Task compensation executed immediately: taskId={}, agentId={}", taskId, agentId);
                } else {
                    log.info("Task compensation queued due to rate limit: taskId={}, agentId={}", taskId, agentId);
                }
                
            } catch (Exception e) {
                log.error("Failed to compensate task start: taskId={}, taskName={}", 
                        task.getId().toHexString(), task.getName(), e);
            }
        }
    }
    
    /**
     * 获取当前轮询间隔
     * 
     * @return 当前轮询间隔（毫秒）
     */
    private long getCurrentInterval() {
        long interval = BASE_INTERVAL_MS + (intervalMultiplier.get() - 1) * INTERVAL_INCREMENT_MS;
        return Math.min(interval, MAX_INTERVAL_MS);
    }
    
    /**
     * 增加轮询间隔
     */
    private void increaseInterval() {
        int currentMultiplier = intervalMultiplier.get();
        long currentInterval = BASE_INTERVAL_MS + (currentMultiplier - 1) * INTERVAL_INCREMENT_MS;
        
        if (currentInterval < MAX_INTERVAL_MS) {
            intervalMultiplier.incrementAndGet();
        }
    }
    
    /**
     * 重置轮询间隔到基础值
     */
    private void resetInterval() {
        intervalMultiplier.set(1);
    }
    
    /**
     * 获取系统用户
     * 
     * @return 系统用户
     */
    private UserDetail getSystemUser() {
        if (systemUser == null) {
            try {
                // 尝试获取系统用户，如果没有则创建一个默认用户
                systemUser = userService.loadUserByUsername("admin");
                if (systemUser == null) {
                    // 创建一个临时的系统用户用于补偿任务
                    systemUser = new UserDetail();
                    systemUser.setUserId("system");
                    systemUser.setUsername("system");
                    systemUser.setEmail("system@tapdata.io");
                }
            } catch (Exception e) {
                log.warn("Failed to get system user, using default: {}", e.getMessage());
                systemUser = new UserDetail();
                systemUser.setUserId("system");
                systemUser.setUsername("system");
                systemUser.setEmail("system@tapdata.io");
            }
        }
        return systemUser;
    }
    
    /**
     * 获取轮询状态信息（用于监控）
     *
     * @return 轮询状态信息
     */
    public String getPollingStatus() {
        long currentInterval = getCurrentInterval();
        long lastPoll = lastPollTime.get();
        long nextPoll = lastPoll + currentInterval;
        long remainingTime = Math.max(0, nextPoll - System.currentTimeMillis());
        int queueSize = taskStartQueueService.getQueueSize();

        return String.format("TaskStartCompensation: interval=%dms, multiplier=%d, queueSize=%d, lastPoll=%d, nextPollIn=%dms",
                currentInterval, intervalMultiplier.get(), queueSize, lastPoll, remainingTime);
    }
    
    /**
     * 手动触发补偿轮询（用于测试或紧急情况）
     */
    public void triggerCompensation() {
        log.info("Manually triggering task start compensation");
        lastPollTime.set(0); // 重置时间，强制下次轮询
        resetInterval(); // 重置间隔
    }
}
