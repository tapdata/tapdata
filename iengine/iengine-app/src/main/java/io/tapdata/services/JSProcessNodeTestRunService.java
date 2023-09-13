package io.tapdata.services;

import com.tapdata.constant.BeanUtil;
import com.tapdata.constant.JSONUtil;
import com.tapdata.tm.commons.schema.MonitoringLogsDto;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.utils.AspectUtils;
import io.tapdata.entity.error.CoreException;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.TaskService;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskService;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.observable.logging.ObsLoggerFactory;
import io.tapdata.observable.logging.appender.ScriptNodeProcessNodeAppender;
import io.tapdata.observable.logging.with.FixedSizeBlockingDeque;
import io.tapdata.service.skeleton.annotation.RemoteService;
import io.tapdata.websocket.handler.TestRunTaskHandler;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author GavinXiao
 * @description JSProcessNodeTestRunService create by Gavin
 * @create 2023/5/11 12:31
 **/
@RemoteService
public class JSProcessNodeTestRunService {

    private final Map<String, TestRunEntity> taskDtoMap = new ConcurrentHashMap<>();
    public static final long cache_time = 3000L;

    public Object testRun(Map<String, Object> events, String nodeId){
        return testRun(events, nodeId, -1);
    }

    public static final int ERROR_REPEAT_EXECUTION  = 18000;
    public Object testRun(Map<String, Object> events, final String nodeId, final int logOutputCount) {
        Map<String, Object> resultMap = null;
        TaskService<TaskDto> taskService = BeanUtil.getBean(HazelcastTaskService.class);
        long startTs = System.currentTimeMillis();
        TaskDto taskDto = JSONUtil.map2POJO(events, TaskDto.class);
        String taskId = taskDto.getId().toHexString();
        ObsLoggerFactory obsLoggerFactory = ObsLoggerFactory.getInstance();

        synchronized (taskDtoMap) {
            if (taskDtoMap.containsKey(taskId)) {
                TestRunEntity testRunEntity = taskDtoMap.get(taskId);
                if (null != testRunEntity && testRunEntity.timestamp + cache_time > System.currentTimeMillis()) {
                    throw new CoreException(ERROR_REPEAT_EXECUTION, "The trial run is currently in progress, please do not repeat it.");
                }
                taskDtoMap.remove(taskId);
            }
            taskDtoMap.put(taskId, new TestRunEntity().add(taskDto));
        }

        if (obsLoggerFactory.inFactory(taskId)) {
            obsLoggerFactory.removeFromFactory(taskId);
        }
        AtomicReference<Object> logCollector = new AtomicReference<>();
        int defaultLogLength = 100;
        if (logOutputCount > 0) {
            defaultLogLength = Math.min(logOutputCount, ScriptNodeProcessNodeAppender.LOG_UPPER_LIMIT);
        }
        FixedSizeBlockingDeque<MonitoringLogsDto> logList = new FixedSizeBlockingDeque<>(defaultLogLength);
        logCollector.set(logList);
        taskDto.taskInfo(ScriptNodeProcessNodeAppender.LOG_LIST_KEY  + taskId, logCollector);
        taskDto.taskInfo(ScriptNodeProcessNodeAppender.MAX_LOG_LENGTH_KEY + taskId, logOutputCount);
        taskDto.taskInfo(ScriptNodeProcessNodeAppender.SCRIPT_NODE_ID_KEY + taskId, nodeId);
        TaskClient<TaskDto> taskClient = null;
        taskDto.setType(ParentTaskDto.TYPE_INITIAL_SYNC);
        ObsLogger logger = obsLoggerFactory.getObsLogger(taskDto);

        AtomicReference<Object> clientResult = new AtomicReference<>();
        try {
            taskClient = taskService.startTestTask(taskDto, clientResult);
            logger.info("{} task start", taskId);
            TestRunTaskHandler.registerTaskClient(taskId, taskClient);
            taskClient.join();
            AspectUtils.executeAspect(new TaskStopAspect().task(taskClient.getTask()).error(taskClient.getError()));
            resultMap = (Map<String, Object>)clientResult.get();
            if(taskClient.getError() != null) {
                throw taskClient.getError();
            }
        } catch (Throwable throwable) {
            if (taskClient != null) {
                AspectUtils.executeAspect(new TaskStopAspect().task(taskClient.getTask()).error(throwable));
            }
            if (null == resultMap)
                resultMap = new HashMap<>();
            resultMap.computeIfAbsent("before", key -> new ArrayList<>());
            resultMap.computeIfAbsent("after", key -> new ArrayList<>());
            resultMap.put("taskId", taskId);
            resultMap.put("ts", new Date().getTime());
            resultMap.put("code", "error");
            resultMap.put("message", throwable.getMessage());
        } finally {
            taskDtoMap.remove(taskId);
            logger.info("test run task {} {}, cost {}ms", taskId, null == taskClient ? "error" : taskClient.getStatus(), (System.currentTimeMillis() - startTs));
        }
        resultMap.put("logs", Optional.ofNullable(logCollector.get()).orElse(new ArrayList<>()));
        return resultMap;
    }

    private static class TestRunEntity {
        long timestamp;
        TaskDto taskDto;

        public TestRunEntity add(TaskDto dto) {
            this.timestamp = System.currentTimeMillis();
            this.taskDto = dto;
            return this;
        }
    }
}
