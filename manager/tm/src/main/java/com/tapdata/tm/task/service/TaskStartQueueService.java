package com.tapdata.tm.task.service;

import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.service.impl.TaskScheduleServiceImpl;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 任务启动队列服务
 * 用于管理被限流的任务启动请求，确保任务按顺序排队启动
 * 
 * @author tapdata
 */
@Slf4j
@Service
public class TaskStartQueueService {
    
    @Autowired
    private TaskOperationRateLimitService taskOperationRateLimitService;
    
    @Autowired
    private TaskScheduleService taskScheduleService;

    @Autowired
    private TaskService taskService;
    
    /**
     * 任务启动请求队列
     */
    private final BlockingQueue<TaskStartRequest> taskStartQueue = new LinkedBlockingQueue<>(1000);
    
    /**
     * 任务启动请求
     */
    @Data
    public static class TaskStartRequest {
        private String taskId;
        private String agentId;
        private UserDetail user;
        private long requestTime;
        private String operationType; // "start", "schedule", "restart"
        
        public TaskStartRequest(String taskId, String agentId, UserDetail user, String operationType) {
            this.taskId = taskId;
            this.agentId = agentId;
            this.user = user;
            this.operationType = operationType;
            this.requestTime = System.currentTimeMillis();
        }
    }
    
    /**
     * 请求启动任务
     * 如果可以立即启动则直接启动，否则加入队列
     * 
     * @param taskId 任务ID
     * @param agentId 引擎ID
     * @param user 用户信息
     * @param operationType 操作类型
     * @return true 如果立即启动，false 如果加入队列
     */
    public boolean requestStartTask(String taskId, String agentId, UserDetail user, String operationType) {
        // 检查是否可以立即启动
        if (taskOperationRateLimitService.canExecuteOperation(taskId, agentId, operationType)) {
            // 可以立即启动
            executeTaskStart(taskId, agentId, user, operationType);
            return true;
        } else {
            // 需要排队
            TaskStartRequest request = new TaskStartRequest(taskId, agentId, user, operationType);
            boolean queued = taskStartQueue.offer(request);
            if (queued) {
                log.info("Task start request queued: taskId={}, agentId={}, operationType={}, queueSize={}", 
                        taskId, agentId, operationType, taskStartQueue.size());
            } else {
                log.error("Task start queue is full, request dropped: taskId={}, agentId={}, operationType={}", 
                        taskId, agentId, operationType);
            }
            return false;
        }
    }
    
    /**
     * 处理队列中的下一个任务启动请求
     * 
     * @return true 如果处理了一个请求，false 如果队列为空
     */
    public boolean processNextTaskStart() {
        TaskStartRequest request = taskStartQueue.poll();
        if (request == null) {
            return false;
        }
        
        // 检查是否可以启动
        if (taskOperationRateLimitService.canExecuteOperation(request.getTaskId(), request.getAgentId(), request.getOperationType())) {
            // 可以启动
            long waitTime = System.currentTimeMillis() - request.getRequestTime();
            log.info("Processing queued task start: taskId={}, agentId={}, operationType={}, waitTime={}ms, remainingQueue={}", 
                    request.getTaskId(), request.getAgentId(), request.getOperationType(), waitTime, taskStartQueue.size());
            
            executeTaskStart(request.getTaskId(), request.getAgentId(), request.getUser(), request.getOperationType());
            return true;
        } else {
            // 仍然被限流，重新放回队列
            taskStartQueue.offer(request);
            log.debug("Task start still rate limited, requeued: taskId={}, agentId={}", 
                    request.getTaskId(), request.getAgentId());
            return false;
        }
    }
    
    /**
     * 执行任务启动
     *
     * @param taskId 任务ID
     * @param agentId 引擎ID
     * @param user 用户信息
     * @param operationType 操作类型
     */
    private void executeTaskStart(String taskId, String agentId, UserDetail user, String operationType) {
        try {
            // 记录操作执行
            taskOperationRateLimitService.recordOperation(taskId, agentId, operationType);

            // 执行实际的启动操作
            switch (operationType) {
                case "start":
                case "restart":
                    // 调用实际的启动方法，避免循环调用
                    if (taskScheduleService instanceof TaskScheduleServiceImpl) {
                        ((TaskScheduleServiceImpl) taskScheduleService).doSendStartMsg(taskId, agentId, user);
                    } else {
                        log.error("TaskScheduleService is not instance of TaskScheduleServiceImpl");
                    }
                    break;
                case "schedule":
                    // 对于调度操作，需要获取TaskDto并调用scheduling方法
                    TaskDto taskDto = taskService.findById(new ObjectId(taskId));
                    if (taskDto != null) {
                        taskScheduleService.scheduling(taskDto, user);
                    } else {
                        log.error("Task not found for scheduling: taskId={}", taskId);
                    }
                    break;
                default:
                    log.warn("Unknown operation type: {}", operationType);
            }

            log.info("Task start executed successfully: taskId={}, agentId={}, operationType={}",
                    taskId, agentId, operationType);

        } catch (Exception e) {
            log.error("Failed to execute task start: taskId={}, agentId={}, operationType={}",
                    taskId, agentId, operationType, e);
        }
    }
    
    /**
     * 获取队列大小
     * 
     * @return 当前队列中等待的任务数量
     */
    public int getQueueSize() {
        return taskStartQueue.size();
    }
    
    /**
     * 获取队列剩余容量
     * 
     * @return 队列剩余容量
     */
    public int getRemainingCapacity() {
        return taskStartQueue.remainingCapacity();
    }
    
    /**
     * 清空队列
     */
    public void clearQueue() {
        int cleared = taskStartQueue.size();
        taskStartQueue.clear();
        log.info("Task start queue cleared, {} requests removed", cleared);
    }
}
