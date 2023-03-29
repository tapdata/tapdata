package io.tapdata.aspect.supervisor;

import io.tapdata.aspect.supervisor.entity.DisposableThreadGroupBase;
import io.tapdata.aspect.utils.AspectUtils;

public class AspectRunnableUtil {
    public static Runnable aspectRunnable(DisposableThreadGroupAspect<? extends DisposableThreadGroupBase> aspect, Runnable runnable){
        return () -> {
            try {
                AspectUtils.executeAspect(DisposableThreadGroupAspect.class, () -> aspect);
                runnable.run();
            } finally {
                AspectUtils.executeAspect(DisposableThreadGroupAspect.class, aspect::release);
            }
        };
    }
}
