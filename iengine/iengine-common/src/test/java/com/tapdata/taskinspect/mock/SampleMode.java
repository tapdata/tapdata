package com.tapdata.taskinspect.mock;

import com.tapdata.taskinspect.IMode;
import com.tapdata.taskinspect.IOperator;
import com.tapdata.taskinspect.TaskInspectContext;

import java.lang.reflect.InvocationTargetException;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/4/16 15:28 Create
 */
public class SampleMode implements IMode {
    protected TaskInspectContext context;
    protected IOperator operator;

    public SampleMode(TaskInspectContext context, IOperator operator) throws Exception {
        if ("ClassNotFoundException".equals(context.getTaskId())) {
            throw new ClassNotFoundException("test-failed");
        } else if ("NoSuchMethodException".equals(context.getTaskId())) {
            throw new NoSuchMethodException("test-failed");
        } else if ("InstantiationException".equals(context.getTaskId())) {
            throw new InstantiationException("test-failed");
        } else if ("IllegalAccessException".equals(context.getTaskId())) {
            throw new IllegalAccessException("test-failed");
        } else if ("InvocationTargetException".equals(context.getTaskId())) {
            throw new InvocationTargetException(new Exception("test-failed"));
        }
        this.context = context;
        this.operator = operator;
    }
}
