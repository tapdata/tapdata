package io.tapdata.threadgroup;

import com.tapdata.entity.task.context.ProcessorBaseContext;

public class ProcessorOnTaskThreadGroup extends ThreadGroup {
    private ProcessorBaseContext processorBaseContext;
    public ProcessorOnTaskThreadGroup(ProcessorBaseContext processorBaseContext) {
        super("ProcessorOnTaskThreadGroup_" + processorBaseContext.getNode().getName());
        this.processorBaseContext = processorBaseContext;
    }

    public ProcessorBaseContext getProcessorBaseContext() {
        return processorBaseContext;
    }

    public void setProcessorBaseContext(ProcessorBaseContext processorBaseContext) {
        this.processorBaseContext = processorBaseContext;
    }
}
