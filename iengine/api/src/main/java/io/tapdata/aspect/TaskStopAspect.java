package io.tapdata.aspect;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.aspect.Aspect;

public class TaskStopAspect extends Aspect {
	private Throwable error;
	public TaskStopAspect error(Throwable error) {
		this.error = error;
		return this;
	}
	private TaskDto task;

	public TaskStopAspect task(TaskDto task) {
		this.task = task;
		return this;
	}

	public TaskDto getTask() {
		return task;
	}

	public void setTask(TaskDto task) {
		this.task = task;
	}

	public Throwable getError() {
		return error;
	}

	public void setError(Throwable error) {
		this.error = error;
	}
}
