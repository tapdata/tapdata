package io.tapdata.observable;

import com.tapdata.entity.TapdataEvent;
import com.tapdata.tm.commons.task.dto.TaskDto;
import io.tapdata.aspect.BatchReadFuncAspect;
import io.tapdata.aspect.StreamReadFuncAspect;
import io.tapdata.aspect.TaskStartAspect;
import io.tapdata.aspect.TaskStopAspect;
import io.tapdata.aspect.task.AspectTask;
import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.entity.aspect.Aspect;
import io.tapdata.entity.aspect.AspectInterceptResult;
import io.tapdata.entity.simplify.pretty.ClassHandlers;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

@AspectTaskSession(includeTypes = {TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC})
public class ObservableAspectTask extends AspectTask {
	private final ClassHandlers observerClassHandlers = new ClassHandlers();
	@Override
	public void onStart(TaskStartAspect startAspect) {
		observerClassHandlers.register(BatchReadFuncAspect.class, this::handleBatchReadFunc);
		observerClassHandlers.register(StreamReadFuncAspect.class, this::handleStreamReadFunc);
	}

	private Void handleStreamReadFunc(StreamReadFuncAspect streamReadFuncAspect) {
//		streamReadFuncAspect.getDataProcessorContext().getPdkAssociateId();
		switch (streamReadFuncAspect.getState()) {
			case StreamReadFuncAspect.STATE_START:
				streamReadFuncAspect.getTime();
//				streamReadFuncAspect.consumer(new Consumer<List<TapdataEvent>>() {
//					@Override
//					public void accept(List<TapdataEvent> tapdataEvents) {
//						System.currentTimeMillis();
//					}
//				});
				break;
			case StreamReadFuncAspect.STATE_STREAM_STARTED:
				streamReadFuncAspect.getStreamStartedTime();
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
//				aspect.consumer(new Consumer<List<TapdataEvent>>() {
//					@Override
//					public void accept(List<TapdataEvent> tapdataEvents) {
//						System.currentTimeMillis();
//					}
//				});
				break;
			case BatchReadFuncAspect.STATE_END:
				aspect.getEndTime();
				break;
		}
		return null;
	}

	@Override
	public void onStop(TaskStopAspect stopAspect) {

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
