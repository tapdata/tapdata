package io.tapdata.aspect;

import io.tapdata.entity.aspect.AspectObserver;
import io.tapdata.entity.aspect.annotations.AspectObserverClass;

@AspectObserverClass(TaskResetAspect.class)
public class TaskResetAspectHandler implements AspectObserver<TaskResetAspect> {
	@Override
	public void observe(TaskResetAspect aspect) {

	}
}
