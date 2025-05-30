package io.tapdata.threadgroup;

import com.tapdata.entity.task.context.DataProcessorContext;

public class ConnectorOnTaskThreadGroup extends ThreadGroup {
    private DataProcessorContext dataProcessorContext;
    public ConnectorOnTaskThreadGroup(DataProcessorContext dataProcessorContext) {
        super("ConnectorOnTaskThreadGroup_" + dataProcessorContext.getNode().getId() + "_" + dataProcessorContext.getNode().getName());
        this.dataProcessorContext = dataProcessorContext;
    }

    public ConnectorOnTaskThreadGroup(DataProcessorContext dataProcessorContext, String threadGroupName) {
        super(threadGroupName);
        this.dataProcessorContext = dataProcessorContext;
    }

    public DataProcessorContext getDataProcessorContext() {
        return dataProcessorContext;
    }

    public void setDataProcessorContext(DataProcessorContext dataProcessorContext) {
        this.dataProcessorContext = dataProcessorContext;
    }
}
