package io.tapdata.aspect;

import io.tapdata.aspect.task.AspectTaskSession;
import io.tapdata.entity.aspect.Aspect;

import java.util.concurrent.atomic.LongAdder;

@AspectTaskSession(includeTypes = "TEST_TARGET")
public class TestSampleTask extends SampleTask {
	LongAdder onStartCounter = new LongAdder();
	LongAdder onStopCounter = new LongAdder();
	ProcessorNodeProcessAspect nodeProcessAspect;

	public TestSampleTask() {
		super();
	}

	@Override
	public void onStart(TaskStartAspect startAspect) {
		//TaskStartAspect
		onStartCounter.increment();
	}

	@Override
	public void onObserveAspect(Aspect aspect) {
		super.onObserveAspect(aspect);
	}

	@Override
	protected Void handleProcessorNodeProcess(ProcessorNodeProcessAspect nodeProcessAspect) {
		this.nodeProcessAspect = nodeProcessAspect;
		return null;
	}

	@Override
	public void onStop(TaskStopAspect stopAspect) {
		//TaskStartAspect
		onStopCounter.increment();
	}
}
