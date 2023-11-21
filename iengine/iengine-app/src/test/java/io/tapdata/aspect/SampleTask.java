package io.tapdata.aspect;

import io.tapdata.aspect.task.AspectTask;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.simplify.pretty.ClassHandlers;

import java.util.ArrayList;
import java.util.List;

public class SampleTask extends AspectTask {
	protected final ClassHandlers observerClassHandlers = new ClassHandlers();

	public SampleTask() {
		observerClassHandlers.register(DataNodeInitAspect.class, this::handleNodeInit);
		observerClassHandlers.register(DataNodeCloseAspect.class, this::handleNodeClose);
		observerClassHandlers.register(ApplicationStartAspect.class, this::handleApplicationStart);
		observerClassHandlers.register(ProcessorNodeProcessAspect.class, this::handleProcessorNodeProcess);
		observerClassHandlers.register(StreamReadFuncAspect.class, this::handleStreamRead);
		observerClassHandlers.register(BatchReadFuncAspect.class, this::handleBatchRead);
		observerClassHandlers.register(TableCountFuncAspect.class, this::handleTableCount);
		observerClassHandlers.register(WriteRecordFuncAspect.class, this::handleWriteRecord);
	}

	protected Void handleWriteRecord(WriteRecordFuncAspect writeRecordFuncAspect) {
		return null;
	}

	protected Void handleTableCount(TableCountFuncAspect tableCountFuncAspect) {
		return null;
	}


	protected Void handleBatchRead(BatchReadFuncAspect batchReadFuncAspect) {
		return null;
	}

	protected Void handleStreamRead(StreamReadFuncAspect streamReadFuncAspect) {
		return null;
	}

	protected Void handleProcessorNodeProcess(ProcessorNodeProcessAspect nodeProcessAspect) {
		return null;
	}

	@Override
	public void onStart(TaskStartAspect startAspect) {
		//TaskStartAspect

	}

	protected Void handleApplicationStart(ApplicationStartAspect applicationStartAspect) {
		return null;
	}

	protected Void handleNodeClose(DataNodeCloseAspect nodeCloseAspect) {
		return null;
	}

	protected Void handleNodeInit(DataNodeInitAspect nodeInitAspect) {
		return null;
	}

	@Override
	public void onStop(TaskStopAspect stopAspect) {
		//TaskStopAspect
	}

	@Override
	public List<Class<? extends Aspect>> observeAspects() {
		List<Class<?>> classes = observerClassHandlers.keyList();
		List<Class<? extends Aspect>> aspectClasses = new ArrayList<>();
		for (Class<?> clazz : classes) {
			aspectClasses.add((Class<? extends Aspect>) clazz);
		}
		return aspectClasses;
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
