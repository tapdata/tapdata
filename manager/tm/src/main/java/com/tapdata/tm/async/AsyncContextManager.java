package com.tapdata.tm.async;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.ServletRequest;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

public class AsyncContextManager {
    private static final Logger logger = LoggerFactory.getLogger(AsyncContextManager.class.getSimpleName());

    private static final AsyncContextManager instance = new AsyncContextManager();

    private Map<String, AsyncJobHolder> jobIdWaiterMap = new ConcurrentHashMap<>();

    public static AsyncContextManager getInstance() {
        return instance;
    }

    public static class AsyncJobHolder<T> implements AsyncListener {
        private AsyncJobWaiter<T> asyncJobWaiter;
        private final ServletRequest servletRequest;
        private final String jobId;

        private long timeoutSeconds;
        private AsyncContext asyncContext;

        public AsyncJobHolder(String jobId, AsyncJobWaiter<T> asyncJobWaiter, ServletRequest servletRequest, long timeoutSeconds) {
            this.jobId = jobId;
            this.asyncJobWaiter = asyncJobWaiter;
            this.servletRequest = servletRequest;
            this.timeoutSeconds = timeoutSeconds;
        }

        public boolean isWaiting() {
            return asyncJobWaiter != null;
        }

        public void start() {
            asyncContext = servletRequest.startAsync();
            asyncContext.setTimeout(this.timeoutSeconds * 1000);
            asyncContext.addListener(this);
        }

        public void close() {
            if(asyncContext != null) {
                asyncContext.complete();
            }
        }

        private void execute(T result, Throwable throwable) {
            if(asyncJobWaiter != null) {
                synchronized (this) {
                    if(asyncJobWaiter != null) {
                        try {
                            asyncJobWaiter.jobAccomplished(result, throwable);
                        } catch(Throwable throwable1) {
                            logger.error("Async Job {} execute failed, {} result {}, throwable {}", jobId, throwable1.getMessage(), result, throwable);

                            //Dangerous to retry with the error only, may cause the jobAccomplished call more than once.
//                            try {
//                                asyncJobWaiter.jobAccomplished(null, throwable1);
//                            } catch(Throwable throwable2) {
//                                logger.error("Async Job {} execute failed, {} result {}, throwable {}", jobId, throwable1.getMessage(), result, throwable);
//                            }
                        } finally {
                            AsyncContextManager.getInstance().jobIdWaiterMap.remove(jobId);
                            asyncJobWaiter = null;
                            close();
                        }
                    }
                }
            }
        }

        @Override
        public void onComplete(AsyncEvent event) throws IOException {
            logger.info("onComplete");
        }

        @Override
        public void onTimeout(AsyncEvent event) throws IOException {
            logger.info("onTimeout");
            execute(null, new TimeoutException("AsyncJob " + jobId + " timeout"));
        }

        @Override
        public void onError(AsyncEvent event) throws IOException {
            logger.info("onError");
            execute(null, event.getThrowable());
        }

        @Override
        public void onStartAsync(AsyncEvent event) throws IOException {
            logger.info("onStartAsync");

        }
        public void executeFailed(Throwable throwable) {
            execute(null, throwable);
        }

        public void executeSuccessfully(T jobResult) {
            execute(jobResult, null);
        }
    }

    /**
     * Register an async job
     *
     * @param jobId job unique id, this id will be used to apply the job result.
     * @param servletRequest Http request, need support async servlet.
     * @param asyncJobWaiter job result will be callback on it
     * @param <T> defined any job result type
     */
    public <T> void registerAsyncJob(String jobId, ServletRequest servletRequest, AsyncJobWaiter<T> asyncJobWaiter) {
        registerAsyncJob(jobId, servletRequest, 45000L, asyncJobWaiter);
    }

    /**
     * Register an async job
     *
     * @param jobId job unique id, this id will be used to apply the job result.
     * @param servletRequest Http request, need support async servlet.
     * @param timeoutSeconds timeout seconds, when timeout, TimeoutException will be received as a result.
     * @param asyncJobWaiter job result will be callback on it
     * @param <T> defined any job result type
     */
    public <T> void registerAsyncJob(String jobId, ServletRequest servletRequest, long timeoutSeconds, AsyncJobWaiter<T> asyncJobWaiter) {
        if(jobId == null)
            throw new IllegalArgumentException("JobId can not be null when register async job");
        if(asyncJobWaiter == null)
            throw new IllegalArgumentException("AsyncJobWaiter can not be null when register async job");
        if(timeoutSeconds < 1)
            throw new IllegalArgumentException("Timeout less than 1 seconds when register async job, " + timeoutSeconds);

        AsyncJobHolder<T> asyncJobHolder = new AsyncJobHolder<>(jobId, asyncJobWaiter, servletRequest, timeoutSeconds);
        AsyncJobHolder<?> old = jobIdWaiterMap.put(jobId, asyncJobHolder);
        if(old != null) {
            old.close();
        }
        asyncJobHolder.start();
    }

    /**
     * Apply the job result base on job id.
     *
     * @param jobId the job id to apply the result, if job not exist, nothing will happen.
     * @param jobResult job result
     * @param throwable job occurred error
     * @param <T> defined any job result type
     */
    public <T> void applyAsyncJobResult(String jobId, T jobResult, Throwable throwable) {
        if(jobId == null)
            throw new IllegalArgumentException("JobId can not be null when apply async job result");
//        if(jobResult == null && throwable == null)
//            throw new IllegalArgumentException("jobResult and throwable can not both be null when apply async job result");

        AsyncJobHolder<T> asyncJobHolder = jobIdWaiterMap.get(jobId);
        if(asyncJobHolder != null && asyncJobHolder.isWaiting()) {
            if(throwable != null) {
                asyncJobHolder.executeFailed(throwable);
            } else {
                asyncJobHolder.executeSuccessfully(jobResult);
            }
        }
    }
}
