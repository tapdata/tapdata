package io.tapdata.services;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.JSONUtil;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.TaskService;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.observable.logging.appender.JSProcessNodeAppender;
import io.tapdata.observable.logging.with.FixedSizeBlockingDeque;
import io.tapdata.service.skeleton.annotation.RemoteService;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author GavinXiao
 * @description JSProcessNodeTestRunService create by Gavin
 * @create 2023/5/11 12:31
 **/
@RemoteService
public class JSProcessNodeTestRunService {

    private final Map<String, TaskDto> taskDtoMap = new ConcurrentHashMap<>();

    public Object testRun(Map<String, Object> events){
        return testRun(events, -1);
    }

    public Object testRun(Map<String, Object> events, final int logOutputCount) {
        TaskService<TaskDto> taskService = BeanUtil.getBean(HazelcastTaskService.class);
        long startTs = System.currentTimeMillis();
        TaskDto taskDto = JSONUtil.map2POJO(events, TaskDto.class);
        AtomicReference<Object> logCollector = new AtomicReference<>();
        int defaultLogLength = 100;
        if (logOutputCount > 0) {
            defaultLogLength = Math.min(logOutputCount, JSProcessNodeAppender.LOG_UPPER_LIMIT);
        }
        FixedSizeBlockingDeque<MonitoringLogsDto> logList = new FixedSizeBlockingDeque<>(defaultLogLength);
        logCollector.set(logList);

        String taskId = taskDto.getId().toHexString();
        taskDto.taskInfo(JSProcessNodeAppender.LOG_LIST_KEY  + taskId, logCollector);
        taskDto.taskInfo(JSProcessNodeAppender.MAX_LOG_LENGTH_KEY + taskId, logOutputCount);

        ObsLogger logger = ObsLoggerFactory.getInstance().getObsLogger(taskDto);
        taskDto.setType(ParentTaskDto.TYPE_INITIAL_SYNC);
        if (taskDtoMap.putIfAbsent(taskId, taskDto) != null) {
            Map<String,Object> paramMap = new HashMap<>();
            paramMap.put("taskId", taskId);
            paramMap.put("ts", new Date().getTime());
            paramMap.put("code", "error");
            paramMap.put("message", taskId + " task is running, skip");
            return paramMap;
        }
        logger.info("{} task start", taskId);
        TaskClient<TaskDto> taskClient = null;
        AtomicReference<Object> clientResult = new AtomicReference<>();
        try {
            taskClient = taskService.startTestTask(taskDto, clientResult);
            taskClient.join();
            AspectUtils.executeAspect(new TaskStopAspect().task(taskClient.getTask()).error(taskClient.getError()));
        } catch (Throwable throwable) {
            logger.error(taskId + " task error", throwable);
            if (taskClient != null) {
                AspectUtils.executeAspect(new TaskStopAspect().task(taskClient.getTask()).error(throwable));
            }
            Map<String,Object> paramMap = new HashMap<>();
            paramMap.put("taskId", taskId);
            paramMap.put("ts", new Date().getTime());
            paramMap.put("code", "error");
            paramMap.put("message", throwable.getMessage());
            return paramMap;
        } finally {
            taskDtoMap.remove(taskId);
            ObsLoggerFactory.getInstance().forceRemoveTaskLogger(taskDto);
        }
        logger.info("test run task {} {}, cost {}ms", taskId, taskClient.getStatus(), (System.currentTimeMillis() - startTs));
        Map<String, Object> resultMap = (Map<String, Object>)clientResult.get();
        resultMap.put("logs", Optional.ofNullable(logCollector.get()).orElse(new ArrayList<>()));
        return resultMap;
    }
}
