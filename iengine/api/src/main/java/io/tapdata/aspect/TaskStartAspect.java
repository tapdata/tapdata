package io.tapdata.aspect;

import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.entity.aspect.Aspect;

public class TaskStartAspect extends Aspect {
	private TaskDto task;
	private ThreadGroup threadGroup;

	public TaskStartAspect task(TaskDto task) {
		this.task = task;
		return this;
	}

	public TaskStartAspect threadGroup(ThreadGroup threadGroup){
		this.threadGroup = threadGroup;
		return this;
	}

	public TaskDto getTask() {
		return task;
	}

	public void setTask(TaskDto task) {
		this.task = task;
	}
}
