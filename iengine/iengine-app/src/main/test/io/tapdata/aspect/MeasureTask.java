package io.tapdata.aspect;

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

	public MeasureTask() {
		observerClassHandlers.register(DataNodeInitAspect.class, this::handleNodeInit);
		observerClassHandlers.register(DataNodeCloseAspect.class, this::handleNodeClose);
		observerClassHandlers.register(ApplicationStartAspect.class, this::handleApplicationStart);
		observerClassHandlers.register(ProcessorNodeProcessAspect.class, this::handleProcessorNodeProcess);
		observerClassHandlers.register(StreamReadFuncAspect.class, this::handleStreamRead);
	}

	private Void handleStreamRead(StreamReadFuncAspect streamReadFuncAspect) {
		return null;
	}

	private Void handleProcessorNodeProcess(ProcessorNodeProcessAspect nodeProcessAspect) {
		return null;
	}

	@Override
	public void onStart() {
		//TaskStartAspect

	}

	private Void handleApplicationStart(ApplicationStartAspect applicationStartAspect) {
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
		return (List<Class<? extends Aspect>>) observerClassHandlers.keyList();
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
