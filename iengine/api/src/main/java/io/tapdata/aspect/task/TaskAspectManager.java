package io.tapdata.aspect.task;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/10/9 16:58 Create
 * @description
 */
public final class TaskAspectManager {
    public static final Map<String, WeakReference<AspectTask>> taskAspectMap = new HashMap<>(16);

    private TaskAspectManager() {

    }

    public static void register(String taskId, AspectTask aspectTask) {
        taskAspectMap.put(taskId, new WeakReference<>(aspectTask));
    }

    public static void remove(String taskId) {
        taskAspectMap.remove(taskId);
    }

    public static AspectTask get(String taskId) {
        return Optional.ofNullable(taskAspectMap.get(taskId)).map(WeakReference::get).orElse(null);
    }
}
