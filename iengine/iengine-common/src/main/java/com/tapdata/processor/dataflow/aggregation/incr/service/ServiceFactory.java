package com.tapdata.processor.dataflow.aggregation.incr.service;

import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.dataflow.ProcessorContext;

public interface ServiceFactory<T> {

	T create(Stage stage, ProcessorContext processorContext);

	Class<T> getServiceType();

}
