package io.tapdata.pdk.apis.functions;

import io.tapdata.pdk.apis.functions.processor.ProcessRecordFunction;

public class ProcessorFunctions implements Functions {
    private ProcessRecordFunction processRecordFunction;


    public ProcessorFunctions withProcessRecordFunction(ProcessRecordFunction function) {
        processRecordFunction = function;
        return this;
    }

    public ProcessRecordFunction getProcessRecordFunction() {
        return processRecordFunction;
    }

    public void setProcessRecordFunction(ProcessRecordFunction processRecordFunction) {
        this.processRecordFunction = processRecordFunction;
    }
}
