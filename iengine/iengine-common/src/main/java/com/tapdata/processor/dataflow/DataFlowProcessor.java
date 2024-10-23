package com.tapdata.processor.dataflow;

import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.Processor;
import io.tapdata.entity.logger.TapLogger;

/**
 * @author jackin
 */
public interface DataFlowProcessor extends Processor {

	void initialize(ProcessorContext context, Stage stage) throws Exception;

	Stage getStage();
	default void logListener(TapLogger.LogListener logListener) {
	}
}
