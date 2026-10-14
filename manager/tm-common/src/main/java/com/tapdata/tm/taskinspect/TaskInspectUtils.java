package com.tapdata.tm.taskinspect;

import com.tapdata.tm.utils.MD5Utils;

import java.util.LinkedHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
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

    int DEFAULT_CORE_POOL_SIZE = 0;
    int DEFAULT_MAX_POOL_SIZE = Integer.MAX_VALUE;
    long DEFAULT_KEEP_ALIVE_SECONDS = 60L;

    /**
     * TaskInspect jobs are long-lived while a task is running. Use direct
     * handoff instead of queueing them behind other long-lived jobs. Workers
     * are reclaimed after the job exits and remains idle for the keep-alive
     * period; task lifecycle code is responsible for stopping the job.
     */
    ExecutorService executorService = createExecutor();

    static ExecutorService createExecutor() {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            DEFAULT_CORE_POOL_SIZE,
            DEFAULT_MAX_POOL_SIZE,
            DEFAULT_KEEP_ALIVE_SECONDS,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            Executors.defaultThreadFactory(),
            new ThreadPoolExecutor.CallerRunsPolicy()
        );
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    static void close(AutoCloseable... closeableArr) throws Exception {
        AtomicReference<Exception> error = new AtomicReference<>(null);
        for (AutoCloseable c : closeableArr) {
            if (null == c) continue;
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

    static void stop(long timeout, BooleanSupplier... suppliers) throws InterruptedException {
        boolean hasFalse = true;
        long start = System.currentTimeMillis();
        while (hasFalse) {
            hasFalse = false;
            for (BooleanSupplier s : suppliers) {
                hasFalse = hasFalse || !s.getAsBoolean();
            }

            long times = System.currentTimeMillis() - start;
            if (times > timeout) {
                throw new RuntimeException("Timeout waiting " + times + "ms for " + TaskInspectUtils.MODULE_NAME + " stop");
            }
            TimeUnit.SECONDS.sleep(1);
        }
    }

    static Future<?> submit(Runnable runnable) {
        return executorService.submit(runnable);
    }

    static String toRowId(String tableName, LinkedHashMap<String, Object> keys) {
        StringBuilder buf = new StringBuilder(tableName);
        for (Object v : keys.values()) {
            buf.append("|").append(v);
        }
        return MD5Utils.toLowerHex(buf.toString());
    }
}
