package io.tapdata.observable.metric.util;

import com.tapdata.tm.commons.task.dto.TaskDto;

import java.util.concurrent.TimeUnit;

/**
 * @author samuel
 * @Description
 * @create 2024-06-28 12:12
 **/
public class TapCompletableFutureTaskEx extends TapCompletableFutureEx {
	private final TaskDto taskDto;

	protected TapCompletableFutureTaskEx(int queueSize, int clearWatermark, TaskDto taskDto) {
		super(queueSize, clearWatermark, String.join("-", RUNNING_JOIN_CHECKER_THREAD_NAME,
				(null != taskDto && null != taskDto.getName()) ? taskDto.getName() : "[no name]", (null != taskDto && null != taskDto.getId() ? taskDto.getId().toString() : "[no id]")));
		this.taskDto = taskDto;
	}

	public static TapCompletableFutureTaskEx create(int queueSize, int joinWatermark, TaskDto taskDto) {
		return new TapCompletableFutureTaskEx(queueSize, joinWatermark, taskDto);
	}

	@Override
	public void stop(long timeout, TimeUnit timeUnit) {
		stopPrivate(timeout, timeUnit, String.join("-", STOP_JOIN_CHECKER_THREAD_NAME,
				(null != taskDto && null != taskDto.getName()) ? taskDto.getName() : "[no name]", (null != taskDto && null != taskDto.getId() ? taskDto.getId().toString() : "[no id]")));
	}

	public TaskDto getTaskDto() {
		return taskDto;
	}
}
