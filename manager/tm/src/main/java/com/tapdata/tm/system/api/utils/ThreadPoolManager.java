package com.tapdata.tm.system.api.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class ThreadPoolManager {
    private static final int CORE_POOL_SIZE = 5;
    private static final int MAX_POOL_SIZE = 20;
    private static final int QUEUE_CAPACITY = 100;
    private static final long KEEP_ALIVE_TIME = 60L;
    private ThreadPoolManager() {}

    private static final ThreadFactory THREAD_FACTORY = r -> {
        Thread thread = new Thread(r);
        thread.setName("Async-task-for-api-debug-" + thread.getId());
        return thread;
    };
    
    private static final RejectedExecutionHandler REJECTION_HANDLER = 
        new ThreadPoolExecutor.CallerRunsPolicy();
    
    private static final ExecutorService ASYNC_TASK_EXECUTOR = 
        new ThreadPoolExecutor(
            CORE_POOL_SIZE,
            MAX_POOL_SIZE,
            KEEP_ALIVE_TIME,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(QUEUE_CAPACITY),
            THREAD_FACTORY,
            REJECTION_HANDLER
        );
    
    public static ExecutorService getAsyncTaskExecutor() {
        return ASYNC_TASK_EXECUTOR;
    }
}