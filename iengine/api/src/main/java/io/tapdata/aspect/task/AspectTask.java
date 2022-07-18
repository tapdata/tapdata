package io.tapdata.aspect.task;

import com.tapdata.tm.commons.task.dto.SubTaskDto;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;

import java.util.List;

public abstract class AspectTask {
	protected SubTaskDto task;
	public abstract void onStart();
	public abstract void onStop();
	public abstract List<Class<? extends Aspect>> observeAspects();
	public abstract List<Class<? extends Aspect>> interceptAspects();

	public abstract void onObserveAspect(Aspect aspect);
	public abstract AspectInterceptResult onInterceptAspect(Aspect aspect);

	public SubTaskDto getTask() {
		return task;
	}

	public void setTask(SubTaskDto task) {
		this.task = task;
	}
}
