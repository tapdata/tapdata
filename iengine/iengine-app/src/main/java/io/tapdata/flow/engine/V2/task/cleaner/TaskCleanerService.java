package io.tapdata.flow.engine.V2.task.cleaner;

import com.tapdata.constant.BeanUtil;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.TaskResetEventDto;
import io.tapdata.flow.engine.V2.schedule.TapdataTaskScheduler;
import io.tapdata.flow.engine.V2.task.OpType;
import io.tapdata.flow.engine.V2.task.TaskClient;
import io.tapdata.flow.engine.V2.task.impl.HazelcastTaskClient;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-10-14 10:43
 **/
public class TaskCleanerService {

	public static void clean(TaskCleanerContext taskCleanerContext, String opTypeStr) throws TaskCleanerException {
		try {
			checkConstructorParamInvalid(taskCleanerContext, opTypeStr);
			checkCanClean(taskCleanerContext.getTaskId());
			taskCleanerContext.opType(OpType.fromOp(opTypeStr));
			String implementClass = taskCleanerContext.getOpType().getImplementClass();
			Constructor<?> constructor;
			try {
				constructor = Class.forName(implementClass).getConstructor(TaskCleanerContext.class);
			} catch (NoSuchMethodException e) {
				throw new TaskCleanerException(String.format("Task cleaner implement class %s not have constructor(TaskCleanerContext.class)", implementClass), e);
			} catch (ClassNotFoundException e) {
				throw new TaskCleanerException("Cannot found task cleaner implement class: " + implementClass, e);
			}
			Object taskCleaner;
			try {
				taskCleaner = constructor.newInstance(taskCleanerContext);
			} catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
				throw new TaskCleanerException("Init task cleaner error: " + e.getMessage(), e);
			}
			if (taskCleaner instanceof TaskCleaner) {
				((TaskCleaner) taskCleaner).clean();
			} else {
				throw new TaskCleanerException(String.format("Task cleaner %s must extends %s", taskCleaner.getClass().getName(), TaskCleaner.class.getName()));
			}
		} catch (TaskCleanerException e) {
//			sendResetTaskLog(taskCleanerContext, e);
			throw e;
		}
	}

	private static void checkConstructorParamInvalid(TaskCleanerContext taskCleanerContext, String opTypeStr) throws TaskCleanerException {
		if (StringUtils.isBlank(opTypeStr)) {
			throw new TaskCleanerException(new IllegalArgumentException("Op type cannot be empty"));
		}
		OpType opType = OpType.fromOp(opTypeStr);
		if (null == opType) {
			throw new TaskCleanerException(new IllegalArgumentException("Op type is invalid: " + opTypeStr));
		}
		if (StringUtils.isBlank(taskCleanerContext.getTaskId())) {
			throw new TaskCleanerException(new IllegalArgumentException("Task id cannot be empty"));
		}
		if (null == taskCleanerContext.getClientMongoOperator()) {
			throw new TaskCleanerException(new IllegalArgumentException("Client operator cannot be null"));
		}
	}

	private static void checkCanClean(String taskId) throws TaskCleanerException {
		TapdataTaskScheduler tapdataTaskScheduler = BeanUtil.getBean(TapdataTaskScheduler.class);
		if (null == tapdataTaskScheduler) {
			return;
		}
		Map<String, TaskClient<TaskDto>> taskClientMap = tapdataTaskScheduler.getTaskClientMap();
		TaskClient<TaskDto> taskDtoTaskClient = taskClientMap.get(taskId);
		if (null != taskDtoTaskClient) {
			if (taskDtoTaskClient instanceof HazelcastTaskClient) {
				throw new TaskCleanerException("Task state data cannot be clean, reason: task is running or stopping, current status: " + taskDtoTaskClient.getStatus() + ", jet job status: " + ((HazelcastTaskClient) taskDtoTaskClient).getJetStatus());
			} else {
				throw new TaskCleanerException("Task state data cannot be clean, reason: task is running or stopping, current status: " + taskDtoTaskClient.getStatus());
			}
		}
	}

	private static void sendResetTaskLog(TaskCleanerContext taskCleanerContext, Throwable throwable) {
		TaskResetEventDto taskResetEventDto = new TaskResetEventDto();
		taskResetEventDto.setDescribe(NodeResetDesc.task_reset_start.name());
		taskResetEventDto.setTaskId(taskResetEventDto.getTaskId());
		taskResetEventDto.setNodeId("");
		taskResetEventDto.setNodeName("");
		taskResetEventDto.setElapsedTime(0L);
		if (null != throwable) {
			taskResetEventDto.failed(throwable);
		} else {
			taskResetEventDto.succeed();
		}
		TaskCleanerReporter taskCleanerReporter = new TaskCleanerReporterImplV1();
		taskCleanerReporter.addEvent(taskCleanerContext.getClientMongoOperator(), taskResetEventDto);
	}
}
