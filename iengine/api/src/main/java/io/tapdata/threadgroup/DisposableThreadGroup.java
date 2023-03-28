package io.tapdata.threadgroup;

import com.tapdata.entity.task.context.DataProcessorContext;

public class DisposableThreadGroup extends ThreadGroup {
    private DataProcessorContext dataProcessorContext;
    public DisposableThreadGroup(String type, String name) {
        super(type+ "_ThreadGroup_" + name);
        //this.dataProcessorContext = dataProcessorContext;
    }

    public DataProcessorContext getDataProcessorContext() {
        return dataProcessorContext;
    }

    public void setDataProcessorContext(DataProcessorContext dataProcessorContext) {
        this.dataProcessorContext = dataProcessorContext;
    }
}
