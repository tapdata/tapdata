package io.tapdata.flow.engine.V2.schedule;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.logger.TapLogger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 任务接管补偿服务
 * 由引擎主动查询状态为启动中且分配给自己的任务进行接管补偿
 * 
 * 补偿条件：
 * 1. 引擎启动10分钟后
 * 2. 且自身已经超过1分钟没有接管新的任务时
 * 
 * 补偿间隔：
 * 1. 初始间隔10秒
 * 2. 如果没有需要补偿的任务，调整为10分钟一次
 * 
 * @author tapdata
 */
@Slf4j
@Service
public class TaskTakeoverCompensationService {
    
    private static final String TAG = TaskTakeoverCompensationService.class.getSimpleName();
    
    /**
     * 引擎启动后需要等待的时间：10分钟
     */
    private static final long ENGINE_STARTUP_WAIT_MS = 10 * 60 * 1000L;
    
    /**
     * 最后接管任务后的等待时间：1分钟
     */
    private static final long LAST_TAKEOVER_WAIT_MS = 1 * 60 * 1000L;
    
    /**
     * 初始补偿间隔：10秒
     */
    private static final long INITIAL_COMPENSATION_INTERVAL_MS = 10 * 1000L;
    
    /**
     * 无任务时的补偿间隔：10分钟
     */
    private static final long NO_TASK_COMPENSATION_INTERVAL_MS = 10 * 60 * 1000L;
    
    private final ClientMongoOperator clientMongoOperator;
    private final TapdataTaskScheduler taskScheduler;
    private final String instanceNo;

    /**
     * 引擎启动时间
     */
    private final AtomicLong engineStartTime = new AtomicLong(System.currentTimeMillis());

    /**
     * 最后一次接管任务的时间
     */
    private final AtomicLong lastTakeoverTime = new AtomicLong(0);

    /**
     * 上次补偿检查时间
     */
    private final AtomicLong lastCompensationTime = new AtomicLong(0);

    /**
     * 当前补偿间隔
     */
    private final AtomicLong currentCompensationInterval = new AtomicLong(INITIAL_COMPENSATION_INTERVAL_MS);

    public TaskTakeoverCompensationService(ClientMongoOperator clientMongoOperator,
                                         TapdataTaskScheduler taskScheduler,
                                         String instanceNo) {
        this.clientMongoOperator = clientMongoOperator;
        this.taskScheduler = taskScheduler;
        this.instanceNo = instanceNo;

        TapLogger.info(TAG, "TaskTakeoverCompensationService initialized for engine: {}", instanceNo);
    }
    
    /**
     * 记录任务接管时间
     */
    public void recordTaskTakeover() {
        lastTakeoverTime.set(System.currentTimeMillis());
        TapLogger.debug(TAG, "Task takeover recorded at: {}", lastTakeoverTime.get());
    }
    
    /**
     * 检查是否需要进行补偿
     * 
     * @return true 如果需要补偿，false 如果不需要
     */
    public boolean shouldCompensate() {
        long currentTime = System.currentTimeMillis();
        
        // 检查引擎启动时间
        if (currentTime - engineStartTime.get() < ENGINE_STARTUP_WAIT_MS) {
            TapLogger.debug(TAG, "Engine not ready for compensation, startup time: {}, current: {}", 
                    engineStartTime.get(), currentTime);
            return false;
        }
        
        // 检查最后接管时间
        long lastTakeover = lastTakeoverTime.get();
        if (lastTakeover > 0 && currentTime - lastTakeover < LAST_TAKEOVER_WAIT_MS) {
            TapLogger.debug(TAG, "Recent takeover detected, last takeover: {}, current: {}", 
                    lastTakeover, currentTime);
            return false;
        }
        
        // 检查补偿间隔
        long lastCompensation = lastCompensationTime.get();
        long interval = currentCompensationInterval.get();
        if (lastCompensation > 0 && currentTime - lastCompensation < interval) {
            TapLogger.debug(TAG, "Compensation interval not reached, last: {}, interval: {}, current: {}", 
                    lastCompensation, interval, currentTime);
            return false;
        }
        
        return true;
    }
    
    /**
     * 执行补偿检查
     */
    public void performCompensation() {
        try {
            long currentTime = System.currentTimeMillis();
            lastCompensationTime.set(currentTime);
            
            TapLogger.info(TAG, "Starting task takeover compensation for engine: {}", instanceNo);
            
            // 查询需要补偿的任务
            List<TaskDto> compensationTasks = findCompensationTasks();
            
            if (compensationTasks.isEmpty()) {
                // 没有需要补偿的任务，调整间隔为10分钟
                currentCompensationInterval.set(NO_TASK_COMPENSATION_INTERVAL_MS);
                TapLogger.info(TAG, "No tasks need compensation, adjusted interval to: {}ms", 
                        NO_TASK_COMPENSATION_INTERVAL_MS);
            } else {
                // 有任务需要补偿，重置间隔为10秒
                currentCompensationInterval.set(INITIAL_COMPENSATION_INTERVAL_MS);
                TapLogger.info(TAG, "Found {} tasks need compensation, reset interval to: {}ms", 
                        compensationTasks.size(), INITIAL_COMPENSATION_INTERVAL_MS);
                
                // 处理补偿任务
                processCompensationTasks(compensationTasks);
            }
            
        } catch (Exception e) {
            TapLogger.error(TAG, "Error during task takeover compensation", e);
        }
    }
    
    /**
     * 查询需要补偿的任务
     * 条件：状态为SCHEDULING，agentId为当前引擎，且超过一定时间
     * 
     * @return 需要补偿的任务列表
     */
    private List<TaskDto> findCompensationTasks() {
        // 查询条件：状态为SCHEDULING，agentId为当前引擎
        Criteria criteria = Criteria.where("status").is(TaskDto.STATUS_SCHEDULING)
                .and("agentId").is(instanceNo);
        
        // 添加时间条件：schedulingTime超过2分钟的任务
        long compensationThreshold = System.currentTimeMillis() - 2 * 60 * 1000L;
        criteria.and("schedulingTime").lt(compensationThreshold);
        
        Query query = new Query(criteria);
        query.fields().include("_id", "name", "agentId", "schedulingTime");

        List<TaskDto> tasks = clientMongoOperator.find(query, ConnectorConstant.TASK_COLLECTION, TaskDto.class);
        
        TapLogger.debug(TAG, "Found {} tasks in SCHEDULING status for engine {} older than 2 minutes", 
                tasks.size(), instanceNo);
        
        return tasks;
    }
    
    /**
     * 处理补偿任务
     * 
     * @param compensationTasks 需要补偿的任务列表
     */
    private void processCompensationTasks(List<TaskDto> compensationTasks) {
        for (TaskDto task : compensationTasks) {
            try {
                String taskId = task.getId().toHexString();
                long schedulingTime = task.getSchedulingTime() != null ? task.getSchedulingTime().getTime() : 0;
                long waitingTime = System.currentTimeMillis() - schedulingTime;
                
                TapLogger.info(TAG, "Compensating task takeover: taskId={}, taskName={}, waitingTime={}ms", 
                        taskId, task.getName(), waitingTime);
                
                // 使用任务调度器进行补偿接管
                taskScheduler.sendStartTask(task);
                
                // 记录接管时间
                recordTaskTakeover();
                
                TapLogger.info(TAG, "Task compensation takeover completed: taskId={}", taskId);
                
                // 只处理一个任务，避免一次性处理太多
                break;
                
            } catch (Exception e) {
                TapLogger.error(TAG, "Failed to compensate task takeover: taskId={}, taskName={}", 
                        task.getId().toHexString(), task.getName(), e);
            }
        }
    }
    
    /**
     * 获取补偿状态信息（用于监控）
     * 
     * @return 补偿状态信息
     */
    public String getCompensationStatus() {
        long currentTime = System.currentTimeMillis();
        long engineUptime = currentTime - engineStartTime.get();
        long lastTakeover = lastTakeoverTime.get();
        long lastCompensation = lastCompensationTime.get();
        long currentInterval = currentCompensationInterval.get();
        
        return String.format("TaskTakeoverCompensation[%s]: uptime=%dms, lastTakeover=%dms, lastCompensation=%dms, interval=%dms", 
                instanceNo, engineUptime, lastTakeover, lastCompensation, currentInterval);
    }
    
    /**
     * 重置引擎启动时间（用于测试）
     */
    public void resetEngineStartTime() {
        engineStartTime.set(System.currentTimeMillis());
        TapLogger.info(TAG, "Engine start time reset for: {}", instanceNo);
    }
    
    /**
     * 手动触发补偿（用于测试或紧急情况）
     */
    public void triggerCompensation() {
        TapLogger.info(TAG, "Manually triggering task takeover compensation for: {}", instanceNo);
        lastCompensationTime.set(0); // 重置时间，强制下次补偿
        currentCompensationInterval.set(INITIAL_COMPENSATION_INTERVAL_MS); // 重置间隔
    }
}
