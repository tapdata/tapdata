package io.tapdata.aspect;

import com.tapdata.entity.task.context.ProcessorBaseContext;
import io.tapdata.entity.aspect.Aspect;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/7/8 14:30 Create
 * @description
 */
public class ProcessorNodeInitErrorAspect extends Aspect {
    Throwable error;
    private ProcessorBaseContext context;

    public ProcessorNodeInitErrorAspect processorContext(ProcessorBaseContext context) {
        this.context = context;
        return this;
    }

    public ProcessorNodeInitErrorAspect error(Throwable error) {
        this.error = error;
        return this;
    }

    public ProcessorBaseContext getProcessorContext() {
        return context;
    }

    public void setProcessorContext(ProcessorBaseContext context) {
        this.context = context;
    }
    public Throwable getError() {
        return error;
    }
    public void setError(Throwable error) {
        this.error = error;
    }
}
