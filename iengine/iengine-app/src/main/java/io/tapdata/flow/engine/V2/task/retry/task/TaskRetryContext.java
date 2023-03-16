package io.tapdata.flow.engine.V2.task.retry.task;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.task.retry.RetryContext;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2023-03-11 17:10
 **/
public class TaskRetryContext extends RetryContext implements Serializable {
	private static final long serialVersionUID = 2391970602675804135L;
	private static final long DEFAULT_METHOD_RETRY_TIME = 3;
	private final TaskDto taskDto;
	private final Long retryDurationMs;
	private Long methodRetryTime;

	private TaskRetryContext(TaskDto taskDto, Long retryDurationMs) {
		if (null == taskDto) {
			throw new IllegalArgumentException("Task dto cannot be null");
		}
		this.taskDto = taskDto;
		if (null != retryDurationMs && retryDurationMs.compareTo(0L) > 0) {
			this.retryDurationMs = retryDurationMs;
		} else {
			this.retryDurationMs = 0L;
		}
		this.methodRetryTime = DEFAULT_METHOD_RETRY_TIME;
	}

	static TaskRetryContext create(TaskDto taskDto) {
		return new TaskRetryContext(taskDto, null);
	}

	static TaskRetryContext create(TaskDto taskDto, Long retryDurationMs) {
		return new TaskRetryContext(taskDto, retryDurationMs);
	}

	public TaskDto getTaskDto() {
		return taskDto;
	}

	public Long getRetryDurationMs() {
		return retryDurationMs;
	}

	public Long getMethodRetryTime() {
		return methodRetryTime;
	}

	public void setMethodRetryTime(Long methodRetryTime) {
		this.methodRetryTime = methodRetryTime;
	}
}
