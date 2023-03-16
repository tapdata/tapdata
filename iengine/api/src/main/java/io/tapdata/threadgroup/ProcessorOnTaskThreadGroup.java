package io.tapdata.threadgroup;

import com.tapdata.entity.task.context.ProcessorBaseContext;

public class ProcessorOnTaskThreadGroup extends ThreadGroup {
    private ProcessorBaseContext processorBaseContext;
    public ProcessorOnTaskThreadGroup(String name) {
        super(name);
    }

    public ProcessorBaseContext getProcessorBaseContext() {
        return processorBaseContext;
    }

    public void setProcessorBaseContext(ProcessorBaseContext processorBaseContext) {
        this.processorBaseContext = processorBaseContext;
    }
}
