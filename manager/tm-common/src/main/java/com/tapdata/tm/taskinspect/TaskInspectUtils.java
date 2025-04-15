package com.tapdata.tm.taskinspect;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/**
 * 任务校验工具类
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/1/18 00:58 Create
 */
public interface TaskInspectUtils {
    String MODULE_NAME = "TaskInspect";

    ExecutorService executorService = Executors.newFixedThreadPool(Integer.MAX_VALUE);

    static void close(AutoCloseable... closeables) throws Exception {
        if (null == closeables) return;

        AtomicReference<Exception> error = new AtomicReference<>(null);
        for (AutoCloseable c : closeables) {
            try {
                c.close();
            } catch (Exception e) {
                if (!error.compareAndSet(null, e)) {
                    error.get().addSuppressed(e);
                }
            }
        }
        if (null != error.get()) {
            throw error.get();
        }
    }

    static void stop(BooleanSupplier stopSupplier, long timeout) throws InterruptedException {
        long start = System.currentTimeMillis();
        while (!stopSupplier.getAsBoolean()) {
            long times = System.currentTimeMillis() - start;
            if (times > timeout) {
                throw new RuntimeException("Timeout waiting " + times + "ms for task-inspect stop");
            }
            TimeUnit.SECONDS.sleep(1);
        }
    }

    static Future<?> submit(Runnable runnable) {
        return executorService.submit(runnable);
    }
}
