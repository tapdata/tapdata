package io.tapdata.observable;

import io.tapdata.aspect.BatchReadAcceptAspect;
import io.tapdata.aspect.BatchReadStartAspect;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;

import java.util.Arrays;
import java.util.List;

@AspectTaskSession
public class ObservableAspectTask extends AspectTask {
	@Override
	public void onStart() {

	}

	@Override
	public void onStop() {

	}

	@Override
	public List<Class<? extends Aspect>> observeAspects() {
		return Arrays.asList(BatchReadAcceptAspect.class, BatchReadStartAspect.class);
	}

	@Override
	public List<Class<? extends Aspect>> interceptAspects() {
		return null;
	}

	@Override
	public void onObserveAspect(Aspect aspect) {

	}

	@Override
	public AspectInterceptResult onInterceptAspect(Aspect aspect) {
		return null;
	}
}
