package io.tapdata.flow.engine.V2.task.retry.task;

import com.tapdata.entity.task.config.TaskConfig;
import com.tapdata.entity.task.context.ProcessorBaseContext;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.task.retry.RetryFactory;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import static io.tapdata.flow.engine.V2.task.retry.task.TaskRetryService.getRetryTimes;

/**
 * @author samuel
 * @Description
 * @create 2023-03-11 17:22
 **/
public class TaskRetryFactory extends RetryFactory implements Serializable {

	private static final long serialVersionUID = -3311313966331701535L;
	private final Map<String, TaskRetryService> taskRetryServiceMap;

	private TaskRetryFactory() {
		this.taskRetryServiceMap = new ConcurrentHashMap<>();
	}

	public static TaskRetryFactory getInstance() {
		return SingleTon.SINGLE_TON.taskRetryFactory;
	}

	@Nonnull
	public TaskRetryService getTaskRetryService(@Nonnull TaskDto taskDto, Long retryDurationMs) {
		return getTaskRetryService(taskDto, retryDurationMs,null, null);
	}

	@Nonnull
	public TaskRetryService getTaskRetryService(@Nonnull TaskDto taskDto, Long retryDurationMs, Long retryIntervalMs, Long methodRetryTime) {
		if (null == taskDto.getId()) {
			throw new IllegalArgumentException("Task id cannot be null");
		}
		String taskId = taskDto.getId().toHexString();
		return taskRetryServiceMap.computeIfAbsent(taskId, k -> {
			TaskRetryContext taskRetryContext = TaskRetryContext.create(taskDto, retryDurationMs, retryIntervalMs);
			if (null != methodRetryTime && methodRetryTime.compareTo(0L) > 0) {
				taskRetryContext.setMethodRetryTime(methodRetryTime);
			}
			return TaskRetryService.create(taskRetryContext);
		});
	}

	public Optional<TaskRetryService> getTaskRetryService(String taskId) {
		if (StringUtils.isBlank(taskId)) {
			return Optional.empty();
		}
		return Optional.ofNullable(taskRetryServiceMap.get(taskId));
	}

	public void removeTaskRetryService(String taskId) {
		if (StringUtils.isBlank(taskId)) {
			return;
		}
		taskRetryServiceMap.remove(taskId);
	}
	public TaskRetryService getTaskRetryService(ProcessorBaseContext processorBaseContext) {
		TaskDto taskDto = processorBaseContext.getTaskDto();
		TaskConfig taskConfig = processorBaseContext.getTaskConfig();
		Long retryIntervalSecond = taskConfig.getTaskRetryConfig().getRetryIntervalSecond();
		long retryIntervalMs = TimeUnit.SECONDS.toMillis(retryIntervalSecond);
		Long taskRetryTimeSecond = taskConfig.getTaskRetryConfig().getMaxRetryTime(TimeUnit.SECONDS);
		long taskRetryDurationMs = TimeUnit.SECONDS.toMillis(taskRetryTimeSecond);
		TaskRetryService taskRetryService = TaskRetryFactory.getInstance().getTaskRetryService(taskDto, taskRetryDurationMs, retryIntervalMs, getRetryTimes(retryIntervalSecond));
		return taskRetryService;
	}

	private enum SingleTon {
		SINGLE_TON,
		;
		private final TaskRetryFactory taskRetryFactory;

		SingleTon() {
			this.taskRetryFactory = new TaskRetryFactory();
		}
	}
}
