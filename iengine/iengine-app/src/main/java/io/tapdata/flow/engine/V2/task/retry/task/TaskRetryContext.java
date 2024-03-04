package io.tapdata.flow.engine.V2.task.retry.task;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.flow.engine.V2.task.retry.RetryContext;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import static io.tapdata.pdk.core.utils.RetryUtils.DEFAULT_RETRY_PERIOD_SECONDS;

/**
 * @author samuel
 * @Description
 * @create 2023-03-11 17:10
 **/
public class TaskRetryContext extends RetryContext implements Serializable {
	private static final long serialVersionUID = 2391970602675804135L;
	private final TaskDto taskDto;
	private final Long retryDurationMs;
	private Long methodRetryTime;

	private Long retryIntervalMs;

	public Long getRetryIntervalMs() {
		return retryIntervalMs;
	}

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
	}

	private TaskRetryContext(TaskDto taskDto, Long retryDurationMs, Long retryIntervalMs) {
		this(taskDto, retryDurationMs);
		if (null != retryIntervalMs && retryIntervalMs.compareTo(0L) > 0) {
			this.retryIntervalMs = retryIntervalMs;
		} else {
			this.retryIntervalMs = TimeUnit.SECONDS.toMillis(DEFAULT_RETRY_PERIOD_SECONDS);
		}
	}

	static TaskRetryContext create(TaskDto taskDto) {
		return new TaskRetryContext(taskDto, null);
	}

	static TaskRetryContext create(TaskDto taskDto, Long retryDurationMs) {
		return new TaskRetryContext(taskDto, retryDurationMs);
	}
	static TaskRetryContext create(TaskDto taskDto, Long retryDurationMs,Long retryIntervalMs){
		return new TaskRetryContext(taskDto,retryDurationMs,retryIntervalMs);
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
