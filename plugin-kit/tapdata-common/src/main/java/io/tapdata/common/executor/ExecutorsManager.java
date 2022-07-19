package io.tapdata.common.executor;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class ExecutorsManager {
    private static final Logger logger = LoggerFactory.getLogger(ExecutorsManager.class);
    private static final String TAG = ExecutorsManager.class.getSimpleName();
    private ExecutorService executorService;
    private ScheduledExecutorService scheduledExecutorService;
    private static final ExecutorsManager instance = new ExecutorsManager();

    private final Object lockScheduled = new int[0];
    private final Object lockExecutor = new int[0];

    private ExecutorsManager() {
    }

    public static ExecutorsManager getInstance() {
        return instance;
    }

    public void initExecutorService(int cpuCores) {
        String coreSizeStr = System.getProperty("agent.thread.core.size");
        int coreSize;
        if(coreSizeStr == null){
            coreSize = cpuCores;
        }else {
            coreSize = Integer.parseInt(coreSizeStr);
        }
        String maximumPoolSizeStr = System.getProperty("agent.thread.maximum.size");
        int maximumPoolSize;
        if(maximumPoolSizeStr == null){
            maximumPoolSize = 10;
        }else {
            maximumPoolSize = Integer.parseInt(maximumPoolSizeStr);
        }
        String keepAliveStr = System.getProperty("agent.thread.keepAlive");
        int keepAlive;
        if(keepAliveStr == null){
            keepAlive = 120;
        }else {
            keepAlive = Integer.parseInt(keepAliveStr);
        }
        String queueSizeStr = System.getProperty("agent.thread.queue.size");
        int queueSize;
        if(queueSizeStr == null){
            queueSize = 100;
        }else {
            queueSize = Integer.parseInt(queueSizeStr);
        }
        ThreadPoolExecutor theExecutorService = new ThreadPoolExecutor(coreSize, maximumPoolSize, keepAlive, TimeUnit.SECONDS, new LinkedBlockingDeque<>(queueSize), new ThreadFactory("OceanusExecutorService-%d"), (r, executor) -> {
            logger.error(TAG, "Thread is rejected, runnable {} pool {}", r, executor.toString());
        });
        theExecutorService.allowCoreThreadTimeOut(true);
        executorService = theExecutorService;
    }

    private void initScheduledExecutorService() {
        String scheduledCoreSizeStr = System.getProperty("agent.scheduled.thread.core.size");
        int scheduledCoreSize;
        if(scheduledCoreSizeStr == null){
            scheduledCoreSize = 2;
        }else {
            scheduledCoreSize = Integer.parseInt(scheduledCoreSizeStr);
        }
        ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(scheduledCoreSize, new ThreadFactory("OceanusScheduledExecutorService-%d"), (r, executor) -> {
            logger.error(TAG, "ScheduledThread is rejected, runnable " + r + " pool " + executor.toString());
        });
        scheduledExecutorService = scheduledThreadPoolExecutor;
    }

    public ExecutorService getExecutorService() {
        if(executorService == null) {
            synchronized (lockExecutor) {
                if(executorService == null) {
                    initExecutorService(2);
                }
            }
        }
        return executorService;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        if(scheduledExecutorService == null) {
            synchronized (lockScheduled) {
                if(scheduledExecutorService == null)
                    initScheduledExecutorService();
            }
        }
        return scheduledExecutorService;
    }

    public ScheduledExecutorService newSingleThreadScheduledExecutor(String name) {
        return Executors.newSingleThreadScheduledExecutor(new ThreadFactory(name));
    }

    public ExecutorService newSingleThreadExecutorService(String name) {
        return Executors.newSingleThreadExecutor(new ThreadFactory(logger.getName()));
    }
}
