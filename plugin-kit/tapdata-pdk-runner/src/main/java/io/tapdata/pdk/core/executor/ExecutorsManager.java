package io.tapdata.pdk.core.executor;


import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.core.utils.CommonUtils;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExecutorsManager {
    private static final String TAG = ExecutorsManager.class.getSimpleName();
    private ExecutorService executorService;
    private ScheduledExecutorService scheduledExecutorService;
    private final AtomicBoolean initDone = new AtomicBoolean(false);

    private static volatile ExecutorsManager instance;
    private ExecutorsManager() {
        init(4);
    }

    public static ExecutorsManager getInstance() {
        if(instance == null) {
            synchronized (ExecutorsManager.class) {
                if(instance == null) {
                    instance = new ExecutorsManager();
                }
            }
        }
        return instance;
    }


    private void init(int cpuCores) {
        if(initDone.compareAndSet(false, true)) {
            int coreSize = CommonUtils.getPropertyInt("pdk_thread_core_size", 10);
            int maximumPoolSize = CommonUtils.getPropertyInt("pdk_thread_maximum_size", 10);
            int keepAlive = CommonUtils.getPropertyInt("pdk_thread_keepAlive", 120);
            int queueSize = CommonUtils.getPropertyInt("pdk_thread_queue_size", 1000);
            ThreadPoolExecutor theExecutorService = new ThreadPoolExecutor(coreSize, maximumPoolSize, keepAlive, TimeUnit.SECONDS, new LinkedBlockingDeque<>(queueSize), new io.tapdata.pdk.core.executor.ThreadFactory("EM"), (r, executor) -> {
                TapLogger.error(TAG, "Thread is rejected, runnable {} pool {}", r, executor);
            });
            theExecutorService.allowCoreThreadTimeOut(true);
            executorService = theExecutorService;

            int scheduledCoreSize = CommonUtils.getPropertyInt("pdk_scheduled_thread_core_size", 4);
            scheduledExecutorService = new ScheduledThreadPoolExecutor(scheduledCoreSize, new io.tapdata.pdk.core.executor.ThreadFactory("EMS"), (r, executor) -> {
                TapLogger.error(TAG, "ScheduledThread is rejected, runnable {} pool {}", r, executor);
            });
        }
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public ScheduledExecutorService newSingleThreadScheduledExecutor(String name) {
        return Executors.newSingleThreadScheduledExecutor(new ThreadFactory(name));
    }

    public ExecutorService newSingleThreadExecutorService(String name) {
        return Executors.newSingleThreadExecutor(new ThreadFactory(name));
    }
}
