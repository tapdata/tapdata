package io.tapdata.aspect;

import com.tapdata.tm.commons.task.dto.SubTaskDto;
import io.tapdata.entity.aspect.Aspect;

public class TaskResetAspect extends Aspect {
	private SubTaskDto task;

	public TaskResetAspect task(SubTaskDto task) {
		this.task = task;
		return this;
	}

	public SubTaskDto getTask() {
		return task;
	}

	public void setTask(SubTaskDto task) {
		this.task = task;
	}
}
