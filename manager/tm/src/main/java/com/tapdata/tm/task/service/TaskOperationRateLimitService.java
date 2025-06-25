package com.tapdata.tm.task.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 任务操作限流服务
 * 用于控制任务启动、停止、重置等操作的频率，避免频繁操作导致资源过载
 * 
 * @author tapdata
 */
@Service
@Slf4j
public class TaskOperationRateLimitService {
    
    /**
     * 操作间隔时间：5秒
     */
    private static final long OPERATION_INTERVAL_MS = 5 * 1000L;

    /**
     * 重试冷却时间：10分钟
     */
    private static final long RETRY_COOLDOWN_MS = 10 * 60 * 1000L;
    
    /**
     * 存储每个引擎的最后操作时间
     * Key: agentId + ":" + operationType, Value: 最后操作时间戳
     */
    private final ConcurrentHashMap<String, AtomicLong> lastOperationTimes = new ConcurrentHashMap<>();
    
    /**
     * 存储每个任务的首次下发完成时间
     * Key: taskId, Value: 首次下发完成时间戳
     */
    private final ConcurrentHashMap<String, AtomicLong> firstDeliveryCompleteTimes = new ConcurrentHashMap<>();
    
    /**
     * 检查是否可以执行任务操作
     *
     * @param taskId 任务ID
     * @param agentId 引擎ID
     * @param operationType 操作类型（start/stop/reset/schedule）
     * @return true 如果可以执行操作，false 如果需要限流
     */
    public boolean canExecuteOperation(String taskId, String agentId, String operationType) {
        long currentTime = System.currentTimeMillis();

        String key = agentId + ":" + operationType;
        AtomicLong lastOperationTime = lastOperationTimes.computeIfAbsent(key, k -> new AtomicLong(0));
        long lastTime = lastOperationTime.get();

        // 检查操作间隔限制
        if (lastTime > 0 && (currentTime - lastTime) < OPERATION_INTERVAL_MS) {
            log.warn("Task operation rate limited: taskId={}, agentId={}, operationType={}, lastOperationTime={}, currentTime={}, intervalMs={}",
                    taskId, agentId, operationType, lastTime, currentTime, currentTime - lastTime);
            return false;
        }

        return true;
    }
    
    /**
     * 检查是否可以执行任务操作（兼容方法，使用任务的agentId）
     *
     * @param taskId 任务ID
     * @param operationType 操作类型（start/stop/reset/schedule）
     * @return true 如果可以执行操作，false 如果需要限流
     */
    public boolean canExecuteOperation(String taskId, String operationType) {
        // 这个方法需要在调用时提供agentId，暂时返回true保持兼容性
        log.warn("Using deprecated canExecuteOperation method without agentId for taskId: {}", taskId);
        return true;
    }

    /**
     * 记录操作执行
     *
     * @param taskId 任务ID
     * @param agentId 引擎ID
     * @param operationType 操作类型
     */
    public void recordOperation(String taskId, String agentId, String operationType) {
        long currentTime = System.currentTimeMillis();
        String key = agentId + ":" + operationType;
        lastOperationTimes.computeIfAbsent(key, k -> new AtomicLong(0)).set(currentTime);

        log.debug("Task operation recorded: taskId={}, agentId={}, operationType={}, timestamp={}",
                taskId, agentId, operationType, currentTime);
    }

    /**
     * 记录操作执行（兼容方法）
     *
     * @param taskId 任务ID
     * @param operationType 操作类型
     */
    public void recordOperation(String taskId, String operationType) {
        log.warn("Using deprecated recordOperation method without agentId for taskId: {}", taskId);
        // 兼容方法，不做实际记录
    }
    
    /**
     * 检查是否可以进行重试操作
     * 
     * @param taskId 任务ID
     * @return true 如果可以重试，false 如果在冷却期内
     */
    public boolean canRetryOperation(String taskId) {
        long currentTime = System.currentTimeMillis();
        
        AtomicLong firstDeliveryTime = firstDeliveryCompleteTimes.get(taskId);
        if (firstDeliveryTime == null) {
            // 如果没有记录首次下发时间，允许重试
            return true;
        }
        
        long firstTime = firstDeliveryTime.get();
        if (firstTime > 0 && (currentTime - firstTime) < RETRY_COOLDOWN_MS) {
            log.debug("Task retry in cooldown period: taskId={}, firstDeliveryTime={}, currentTime={}, cooldownRemainingMs={}", 
                    taskId, firstTime, currentTime, RETRY_COOLDOWN_MS - (currentTime - firstTime));
            return false;
        }
        
        return true;
    }
    
    /**
     * 记录首次下发完成时间
     * 
     * @param taskId 任务ID
     */
    public void recordFirstDeliveryComplete(String taskId) {
        long currentTime = System.currentTimeMillis();
        firstDeliveryCompleteTimes.computeIfAbsent(taskId, k -> new AtomicLong(0)).set(currentTime);
        
        log.debug("First delivery complete recorded: taskId={}, timestamp={}", taskId, currentTime);
    }
    
    /**
     * 清理引擎的限流记录
     *
     * @param agentId 引擎ID
     */
    public void clearAgentRecords(String agentId) {
        lastOperationTimes.entrySet().removeIf(entry -> entry.getKey().startsWith(agentId + ":"));

        log.debug("Agent rate limit records cleared: agentId={}", agentId);
    }

    /**
     * 清理任务的限流记录（兼容方法）
     *
     * @param taskId 任务ID
     */
    public void clearTaskRecords(String taskId) {
        firstDeliveryCompleteTimes.remove(taskId);

        log.debug("Task rate limit records cleared: taskId={}", taskId);
    }
    
    /**
     * 获取引擎的最后操作时间
     *
     * @param agentId 引擎ID
     * @param operationType 操作类型
     * @return 最后操作时间戳，如果没有记录则返回0
     */
    public long getLastOperationTime(String agentId, String operationType) {
        String key = agentId + ":" + operationType;
        AtomicLong lastTime = lastOperationTimes.get(key);
        return lastTime != null ? lastTime.get() : 0;
    }

    /**
     * 获取任务的最后操作时间（兼容方法）
     *
     * @param taskId 任务ID
     * @return 最后操作时间戳，如果没有记录则返回0
     */
    public long getLastOperationTime(String taskId) {
        log.warn("Using deprecated getLastOperationTime method without agentId for taskId: {}", taskId);
        return 0;
    }
    
    /**
     * 获取任务的首次下发完成时间
     * 
     * @param taskId 任务ID
     * @return 首次下发完成时间戳，如果没有记录则返回0
     */
    public long getFirstDeliveryCompleteTime(String taskId) {
        AtomicLong firstTime = firstDeliveryCompleteTimes.get(taskId);
        return firstTime != null ? firstTime.get() : 0;
    }
    
    /**
     * 清理过期的记录（可选的定期清理方法）
     * 清理超过24小时的记录以避免内存泄漏
     */
    public void cleanupExpiredRecords() {
        long currentTime = System.currentTimeMillis();
        long expireTime = 24 * 60 * 60 * 1000L; // 24小时
        
        lastOperationTimes.entrySet().removeIf(entry -> 
            (currentTime - entry.getValue().get()) > expireTime);
        
        firstDeliveryCompleteTimes.entrySet().removeIf(entry -> 
            (currentTime - entry.getValue().get()) > expireTime);
        
        log.debug("Expired rate limit records cleaned up");
    }
}
