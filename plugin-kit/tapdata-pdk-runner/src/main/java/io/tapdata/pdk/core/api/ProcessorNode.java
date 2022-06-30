package io.tapdata.pdk.core.api;

import io.tapdata.pdk.apis.TapProcessor;
import io.tapdata.pdk.apis.context.TapProcessorContext;
import io.tapdata.pdk.apis.functions.ProcessorFunctions;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;

public class ProcessorNode extends Node {
    TapProcessor processor;
    TapProcessorContext processorContext;

    ProcessorFunctions processorFunctions;

    void processorFunctions(ProcessorFunctions processorFunctions) throws Throwable {
        processor.processorFunctions(processorFunctions);
    }


    public TapProcessorContext getProcessorContext() {
        return processorContext;
    }

    public TapProcessor getProcessor() {
        return processor;
    }

    public ProcessorFunctions getProcessorFunctions() {
        return processorFunctions;
    }

}
