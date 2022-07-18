package io.tapdata.aspect;

import com.tapdata.entity.task.context.DataProcessorContext;
import io.tapdata.entity.aspect.Aspect;

public abstract class DataNodeAspect<T extends DataNodeAspect<?>> extends Aspect {
	private DataProcessorContext dataProcessorContext;

	public T dataProcessorContext(DataProcessorContext dataProcessorContext) {
		this.dataProcessorContext = dataProcessorContext;
		return (T) this;
	}

	public DataProcessorContext getDataProcessorContext() {
		return dataProcessorContext;
	}

	public void setDataProcessorContext(DataProcessorContext dataProcessorContext) {
		this.dataProcessorContext = dataProcessorContext;
	}
}
