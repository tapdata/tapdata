package com.tapdata.tm.base.exception;

/**
 * 任务调度限流异常
 * 当任务调度操作被限流时抛出此异常
 * 
 * @author tapdata
 */
public class TaskScheduleRateLimitException extends BizException {
    
    public TaskScheduleRateLimitException() {
        super("Task.ScheduleRateLimit");
    }
    
    public TaskScheduleRateLimitException(String taskId) {
        super("Task.ScheduleRateLimit", taskId);
    }
    
    public TaskScheduleRateLimitException(String message, String taskId) {
        super(message, taskId);
    }
}
