package io.tapdata.flow.engine.V2.schedule;

import com.tapdata.tm.commons.task.dto.TaskDto;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 引擎端任务启动限流服务
 * 用于控制任务启动的频率，确保10秒内只有一个任务会进入启动流程
 * 支持任务排队，重试任务不受限流影响
 *
 * @author tapdata
 */
@Service
public class TaskStartRateLimitService {

    private static final Logger logger = LogManager.getLogger(TaskStartRateLimitService.class);

    /**
     * 任务启动间隔时间：10秒
     */
    private static final long START_INTERVAL_MS = 10 * 1000L;

    /**
     * 最后一次任务启动时间
     */
    private final AtomicLong lastStartTime = new AtomicLong(0);

    /**
     * 等待启动的任务队列
     */
    private final BlockingQueue<TaskStartRequest> pendingTasks = new LinkedBlockingQueue<>();

    /**
     * 任务启动请求
     */
    public static class TaskStartRequest {
        private final TaskDto taskDto;
        private final boolean isRetry;
        private final long requestTime;

        public TaskStartRequest(TaskDto taskDto, boolean isRetry) {
            this.taskDto = taskDto;
            this.isRetry = isRetry;
            this.requestTime = System.currentTimeMillis();
        }

        public TaskDto getTaskDto() { return taskDto; }
        public boolean isRetry() { return isRetry; }
        public long getRequestTime() { return requestTime; }
    }
    
    /**
     * 请求启动任务
     * 重试任务立即启动，普通任务检查限流
     *
     * @param taskDto 任务DTO
     * @param isRetry 是否为重试任务
     * @return true 如果可以立即启动，false 如果需要排队
     */
    public boolean requestStartTask(TaskDto taskDto, boolean isRetry) {
        String taskId = taskDto.getId().toHexString();
        String taskName = taskDto.getName();

        // 重试任务不受限流影响，立即启动
        if (isRetry) {
            recordTaskStart(taskId, taskName);
            logger.info("Task retry start immediately: taskId={}, taskName={}", taskId, taskName);
            return true;
        }

        long currentTime = System.currentTimeMillis();
        long lastTime = lastStartTime.get();

        // 检查启动间隔限制
        if (lastTime > 0 && (currentTime - lastTime) < START_INTERVAL_MS) {
            // 需要排队等待
            TaskStartRequest request = new TaskStartRequest(taskDto, false);
            pendingTasks.offer(request);
            long remainingTime = START_INTERVAL_MS - (currentTime - lastTime);
            logger.info("Task start queued: taskId={}, taskName={}, queueSize={}, remainingMs={}",
                    taskId, taskName, pendingTasks.size(), remainingTime);
            return false;
        }

        // 可以立即启动
        recordTaskStart(taskId, taskName);
        return true;
    }

    /**
     * 获取下一个等待启动的任务
     *
     * @return 下一个可以启动的任务，如果没有则返回null
     */
    public TaskDto getNextPendingTask() {
        long currentTime = System.currentTimeMillis();
        long lastTime = lastStartTime.get();

        // 检查是否可以启动下一个任务
        if (lastTime > 0 && (currentTime - lastTime) < START_INTERVAL_MS) {
            return null;
        }

        TaskStartRequest request = pendingTasks.poll();
        if (request != null) {
            TaskDto taskDto = request.getTaskDto();
            // 不在这里记录启动时间，让实际启动时记录
            logger.info("Task dequeued for start: taskId={}, taskName={}, waitTime={}ms, queueSize={}",
                    taskDto.getId().toHexString(), taskDto.getName(),
                    currentTime - request.getRequestTime(), pendingTasks.size());
            return taskDto;
        }

        return null;
    }

    /**
     * 记录排队任务的启动（公开方法）
     *
     * @param taskId 任务ID
     * @param taskName 任务名称
     */
    public void recordQueuedTaskStart(String taskId, String taskName) {
        recordTaskStart(taskId, taskName);
        logger.info("Queued task start recorded: taskId={}, taskName={}", taskId, taskName);
    }

    /**
     * 重新将任务放入队列
     *
     * @param taskDto 需要重新排队的任务
     */
    public void requeueTask(TaskDto taskDto) {
        TaskStartRequest request = new TaskStartRequest(taskDto, false);
        pendingTasks.offer(request);
        logger.info("Task requeued: taskId={}, taskName={}, queueSize={}",
                taskDto.getId().toHexString(), taskDto.getName(), pendingTasks.size());
    }

    /**
     * 记录任务启动
     *
     * @param taskId 任务ID
     * @param taskName 任务名称
     */
    private void recordTaskStart(String taskId, String taskName) {
        long currentTime = System.currentTimeMillis();
        lastStartTime.set(currentTime);

        logger.debug("Task start recorded: taskId={}, taskName={}, timestamp={}",
                taskId, taskName, currentTime);
    }
    
    /**
     * 获取等待队列大小
     *
     * @return 等待启动的任务数量
     */
    public int getPendingTaskCount() {
        return pendingTasks.size();
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
     * 清空等待队列
     */
    public void clearPendingTasks() {
        int cleared = pendingTasks.size();
        pendingTasks.clear();
        logger.info("Cleared {} pending tasks from queue", cleared);
    }

    /**
     * 重置限流状态（用于测试或特殊情况）
     */
    public void reset() {
        lastStartTime.set(0);
        clearPendingTasks();
        logger.info("Task start rate limit state reset");
    }
}
