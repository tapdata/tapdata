package io.tapdata.observable;

import io.tapdata.aspect.BatchReadFuncAspect;
import io.tapdata.aspect.StreamReadFuncAspect;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.simplify.pretty.ClassHandlers;

import java.util.Arrays;
import java.util.List;

@AspectTaskSession
public class ObservableAspectTask extends AspectTask {
	private final ClassHandlers observerClassHandlers = new ClassHandlers();
	@Override
	public void onStart() {
		observerClassHandlers.register(BatchReadFuncAspect.class, this::handleBatchReadFunc);
		observerClassHandlers.register(StreamReadFuncAspect.class, this::handleStreamReadFunc);
	}

	private Void handleStreamReadFunc(StreamReadFuncAspect streamReadFuncAspect) {
		switch (streamReadFuncAspect.getState()) {
			case StreamReadFuncAspect.STATE_START:
				streamReadFuncAspect.getTime();
				break;
			case StreamReadFuncAspect.STATE_STREAM_STARTED:
				streamReadFuncAspect.getStreamStartedTime();
				break;
			case StreamReadFuncAspect.STATE_ACCEPT:
				streamReadFuncAspect.getAcceptTime();
				break;
			case StreamReadFuncAspect.STATE_END:
				streamReadFuncAspect.getEndTime();
				break;
		}
		return null;
	}

	private Void handleBatchReadFunc(BatchReadFuncAspect aspect) {
		switch (aspect.getState()) {
			case BatchReadFuncAspect.STATE_START:
				aspect.getTime();
				break;
			case BatchReadFuncAspect.STATE_ACCEPT:
				aspect.getEvents();
				aspect.getAcceptTime();
				break;
			case BatchReadFuncAspect.STATE_END:
				aspect.getEndTime();
				break;
		}
		return null;
	}

	@Override
	public void onStop() {

	}

	@Override
	public List<Class<? extends Aspect>> observeAspects() {
		return Arrays.asList(BatchReadFuncAspect.class, StreamReadFuncAspect.class);
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
