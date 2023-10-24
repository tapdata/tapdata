package io.tapdata.pdk.core.utils.state;


import io.tapdata.entity.logger.TapLogger;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class StateOperateRetryHandler<K, T> {
    private static final String TAG = StateOperateRetryHandler.class.getSimpleName();
    private int retryCount = 0;
    private int MAX_RETRY = 5;
    private long RETRY_INTERVAL = 1000L;
    private ScheduledFuture<?> retryTask;
    private OperateListener<K, T> operateListener, initializingListener;
    private OperateFailedListener<K, T> operateFailedListener;
    private ScheduledExecutorService scheduledExecutorService;
    private OperateFailedOccurError<K, T> operateFailedOccurError;
    private StateMachine<K, T> stateMachine;

    private StateOperateRetryHandler(StateMachine<K, T> stateMachine, ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.stateMachine = stateMachine;
    }

    public static <K, T> StateOperateRetryHandler<K, T> build(StateMachine<K, T> stateMachine, ScheduledExecutorService scheduledExecutorService) {
        return new StateOperateRetryHandler<K, T>(stateMachine, scheduledExecutorService);
    }

    public StateOperateRetryHandler<K, T> setMaxRetry(int maxRetry) {
        this.MAX_RETRY = maxRetry;
        return this;
    }

    public StateOperateRetryHandler<K, T> setRetryInterval(long retryInterval) {
        this.RETRY_INTERVAL = retryInterval;
        return this;
    }

    public StateOperateRetryHandler<K, T> setOperateListener(OperateListener<K, T> operateListener) {
        this.operateListener = operateListener;
        return this;
    }

    public StateOperateRetryHandler<K, T> setInitializingListener(OperateListener<K, T> initializingListener) {
        this.initializingListener = initializingListener;
        return this;
    }

    public StateOperateRetryHandler<K, T> setOperateFailedListener(OperateFailedListener<K, T> operateFailedListener) {
        this.operateFailedListener = operateFailedListener;
        return this;
    }

    public StateOperateRetryHandler<K, T> setOperateFailedOccurError(OperateFailedOccurError<K, T> operateFailedOccurError) {
        this.operateFailedOccurError = operateFailedOccurError;
        return this;
    }

    public void operate(T t, StateMachine<K, T> stateMachine) throws Throwable {
        if(operateListener != null) {
            operateListener.operate(t, stateMachine);
        }
    }

    public void operateFailed(T tt, StateMachine<K, T> stateMachine) {
        if(operateFailedListener != null) {
            releaseRetryTask();
            if(retryTask == null) {
                retryTask = scheduledExecutorService.schedule(() -> {
                    retryCount++;
                    if(retryCount > MAX_RETRY) {
                        try {
                            operateFailedListener.operate(false, retryCount, MAX_RETRY, tt, stateMachine);
                        } catch(Throwable t) {
                            t.printStackTrace();
                            if(operateFailedOccurError != null) {
                                try {
                                    operateFailedOccurError.operate(false, retryCount, MAX_RETRY, t, tt, stateMachine);
                                } catch (Throwable throwable) {
                                    throwable.printStackTrace();
                                    TapLogger.error(TAG, "operateFailedOccurError failed, " + throwable.getMessage());
                                }
                            }
                        }
                    } else {
                        try {
                            operateFailedListener.operate(true, retryCount, MAX_RETRY, tt, stateMachine);
                        } catch (Throwable t) {
                            t.printStackTrace();
                            if(operateFailedOccurError != null) {
                                try {
                                    operateFailedOccurError.operate(true, retryCount, MAX_RETRY, t, tt, stateMachine);
                                } catch (Throwable throwable) {
                                    throwable.printStackTrace();
                                    TapLogger.error(TAG, "operateFailedOccurError failed, " + throwable.getMessage());
                                }
                            }
                        }
                    }
                }, RETRY_INTERVAL, TimeUnit.MILLISECONDS);
            }
        }
    }

    public void initializing(T t, StateMachine<K, T> stateMachine) throws Throwable {
        releaseRetryTask();
        retryCount = 0;
        if(initializingListener != null)
            initializingListener.operate(t, stateMachine);
    }

    private void releaseRetryTask() {
        if(retryTask != null) {
            retryTask.cancel(true);
            retryTask = null;
        }
    }

    public interface OperateListener<K, T> {
        void operate(T t, StateMachine<K, T> stateMachine) throws Throwable;
    }
    public interface OperateFailedListener<K, T> {
        void operate(boolean willRetry, int retryCount, int maxRetry, T t, StateMachine<K, T> stateMachine) throws Throwable;
    }

    public interface OperateFailedOccurError<K, T> {
        void operate(boolean willRetry, int retryCount, int maxRetry, Throwable throwable, T t, StateMachine<K, T> stateMachine) throws Throwable;
    }
}
