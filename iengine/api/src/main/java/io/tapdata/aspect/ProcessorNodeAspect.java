package io.tapdata.aspect;

import com.tapdata.entity.task.context.ProcessorBaseContext;
import io.tapdata.entity.aspect.Aspect;

public abstract class ProcessorNodeAspect<T extends ProcessorNodeAspect<?>> extends Aspect {
	private ProcessorBaseContext processorBaseContext;

	public T processorBaseContext(ProcessorBaseContext processorBaseContext) {
		this.processorBaseContext = processorBaseContext;
		return (T) this;
	}

	public ProcessorBaseContext getProcessorBaseContext() {
		return processorBaseContext;
	}

	public void setProcessorBaseContext(ProcessorBaseContext processorBaseContext) {
		this.processorBaseContext = processorBaseContext;
	}
}
