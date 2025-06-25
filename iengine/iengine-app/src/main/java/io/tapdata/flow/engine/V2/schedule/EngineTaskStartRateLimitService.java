package io.tapdata.flow.engine.V2.schedule;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 引擎端任务启动限流服务
 * 用于控制引擎端直接启动任务的频率，确保10秒内只启动一个任务
 * 
 * @author tapdata
 */
@Service
public class EngineTaskStartRateLimitService {
    
    private static final Logger logger = LogManager.getLogger(EngineTaskStartRateLimitService.class);
    
    /**
     * 任务启动间隔时间：10秒
     */
    private static final long START_INTERVAL_MS = 10 * 1000L;
    
    /**
     * 最后一次任务启动时间
     */
    private final AtomicLong lastStartTime = new AtomicLong(0);
    
    /**
     * 检查是否可以启动任务
     * 
     * @param taskId 任务ID
     * @param taskName 任务名称
     * @return true 如果可以启动任务，false 如果需要限流
     */
    public boolean canStartTask(String taskId, String taskName) {
        long currentTime = System.currentTimeMillis();
        long lastTime = lastStartTime.get();
        
        // 检查启动间隔限制
        if (lastTime > 0 && (currentTime - lastTime) < START_INTERVAL_MS) {
            long remainingTime = START_INTERVAL_MS - (currentTime - lastTime);
            logger.warn("Engine task start rate limited: taskId={}, taskName={}, lastStartTime={}, currentTime={}, remainingMs={}", 
                    taskId, taskName, lastTime, currentTime, remainingTime);
            return false;
        }
        
        return true;
    }
    
    /**
     * 记录任务启动
     * 
     * @param taskId 任务ID
     * @param taskName 任务名称
     */
    public void recordTaskStart(String taskId, String taskName) {
        long currentTime = System.currentTimeMillis();
        lastStartTime.set(currentTime);
        
        logger.info("Engine task start recorded: taskId={}, taskName={}, timestamp={}", 
                taskId, taskName, currentTime);
    }
    
    /**
     * 获取最后一次任务启动时间
     * 
     * @return 最后一次任务启动时间戳，如果没有记录则返回0
     */
    public long getLastStartTime() {
        return lastStartTime.get();
    }
    
    /**
     * 获取距离下次可以启动任务的剩余时间
     * 
     * @return 剩余时间（毫秒），如果可以立即启动则返回0
     */
    public long getRemainingTime() {
        long currentTime = System.currentTimeMillis();
        long lastTime = lastStartTime.get();
        
        if (lastTime == 0) {
            return 0;
        }
        
        long elapsed = currentTime - lastTime;
        if (elapsed >= START_INTERVAL_MS) {
            return 0;
        }
        
        return START_INTERVAL_MS - elapsed;
    }
    
    /**
     * 重置限流状态（用于测试或特殊情况）
     */
    public void reset() {
        lastStartTime.set(0);
        logger.info("Engine task start rate limit state reset");
    }
}
