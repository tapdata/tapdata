package io.tapdata.supervisor.util;

import io.tapdata.threadgroup.ConnectorOnTaskThreadGroup;
import io.tapdata.threadgroup.DisposableThreadGroup;
import io.tapdata.threadgroup.ProcessorOnTaskThreadGroup;
import io.tapdata.threadgroup.TaskThreadGroup;
import io.tapdata.threadgroup.utils.ThreadGroupUtil;

public class TapTaskThreadGroupUtil {
    private static final Class<? extends ThreadGroup>[] DEFAULT_TASK_THREAD = new Class[]{TaskThreadGroup.class};
    private static final Class<? extends ThreadGroup>[] DEFAULT_NODE_THREAD = new Class[]{ProcessorOnTaskThreadGroup.class, ConnectorOnTaskThreadGroup.class, DisposableThreadGroup.class};

    public static ThreadGroupUtil getDefaultThreadUtil() {
        return ThreadGroupUtil.create(DEFAULT_TASK_THREAD[0], DEFAULT_NODE_THREAD);
    }

    public static Class<? extends ThreadGroup>[] getDefaultTaskThreadGroupClass() {
        return DEFAULT_TASK_THREAD;
    }

    public static Class<? extends ThreadGroup>[] getDefaultNodeThreadGroupClass() {
        return DEFAULT_NODE_THREAD;
    }

}
