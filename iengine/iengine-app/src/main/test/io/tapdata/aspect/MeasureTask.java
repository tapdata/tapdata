package io.tapdata.aspect;

import io.tapdata.aspect.ApplicationStartAspect;
import io.tapdata.aspect.DataNodeCloseAspect;
import io.tapdata.aspect.DataNodeInitAspect;
import io.tapdata.aspect.StreamReadDataNodeAspect;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.simplify.pretty.ClassHandlers;

import java.util.Arrays;
import java.util.List;

@AspectTaskSession
public class MeasureTask extends AspectTask {
	private final ClassHandlers observerClassHandlers = new ClassHandlers();

	@Override
	public void onStart() {
		//TaskStartAspect
		observerClassHandlers.register(DataNodeInitAspect.class, this::handleNodeInit);
		observerClassHandlers.register(DataNodeCloseAspect.class, this::handleNodeClose);
		observerClassHandlers.register(StreamReadDataNodeAspect.class, this::handleStreamReadNode);
		observerClassHandlers.register(ApplicationStartAspect.class, this::handleApplicationStart);
	}

	private Void handleApplicationStart(ApplicationStartAspect applicationStartAspect) {
		return null;
	}

	private Void handleStreamReadNode(StreamReadDataNodeAspect streamReadNodeAspect) {
		return null;
	}

	private Void handleNodeClose(DataNodeCloseAspect nodeCloseAspect) {
		return null;
	}

	private Void handleNodeInit(DataNodeInitAspect nodeInitAspect) {
		return null;
	}

	@Override
	public void onStop() {
		//TaskStopAspect
	}

	@Override
	public List<Class<? extends Aspect>> observeAspects() {
		return Arrays.asList(DataNodeInitAspect.class, DataNodeCloseAspect.class, StreamReadDataNodeAspect.class, ApplicationStartAspect.class);
	}

	@Override
	public List<Class<? extends Aspect>> interceptAspects() {
		return null;
	}

	@Override
	public void onObserveAspect(Aspect aspect) {
		observerClassHandlers.handle(aspect);
	}


	@Override
	public AspectInterceptResult onInterceptAspect(Aspect aspect) {
		return null;
	}
}
