package io.tapdata.aspect;

import com.tapdata.tm.commons.task.dto.SubTaskDto;
import io.tapdata.entity.aspect.Aspect;

public class TaskStopAspect extends Aspect {
	private Throwable error;
	public TaskStopAspect error(Throwable error) {
		this.error = error;
		return this;
	}
	private SubTaskDto task;

	public TaskStopAspect task(SubTaskDto task) {
		this.task = task;
		return this;
	}

	public SubTaskDto getTask() {
		return task;
	}

	public void setTask(SubTaskDto task) {
		this.task = task;
	}

	public Throwable getError() {
		return error;
	}

	public void setError(Throwable error) {
		this.error = error;
	}
}
