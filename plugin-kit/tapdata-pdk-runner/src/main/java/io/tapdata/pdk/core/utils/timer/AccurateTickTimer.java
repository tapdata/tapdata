package io.tapdata.pdk.core.utils.timer;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.core.executor.ExecutorsManager;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class AccurateTickTimer {
    public static final String TAG = AccurateTickTimer.class.getSimpleName();
    private long period;
    private InnerRunnable runnable;
    private String description;
    private AtomicBoolean isStarted = new AtomicBoolean(false);
    private AtomicBoolean isStopped = new AtomicBoolean(false);
    private Long delayMilliseconds;
    private AccurateTickFinishListener finishListener;

    public class InnerRunnable implements Runnable {
        private Runnable runnable;
        private long takes = 0;
        InnerRunnable(Runnable runnable) {
            this.runnable = runnable;
        }
        @Override
        public void run() {
            try {
                if(isStopped.get()) {
                    callFinishListener();
                    return;
                }
                long time = System.currentTimeMillis();
                runnable.run();
                takes = System.currentTimeMillis() - time;
            } catch (Throwable throwable) {
                throwable.printStackTrace();
                TapLogger.error(TAG, AccurateTickTimer.this.description + " occurred error " + throwable.getMessage());
            } finally {
                long thePeriod = period - takes;
                if(thePeriod < 0) {
                    thePeriod = 0;
                }
                if(!isStopped.get()) {
                    ExecutorsManager.getInstance().getScheduledExecutorService().schedule(this, thePeriod, TimeUnit.MILLISECONDS);
                } else {
                    callFinishListener();
                }
            }
        }
    }

    public AccurateTickTimer(String description, long period, Runnable runnable) {
        this(description, period, runnable, null);
    }
    public AccurateTickTimer(String description, long period, Runnable runnable, Long delayMilliseconds) {
        this.delayMilliseconds = delayMilliseconds;
        this.period = period;
        this.runnable = new InnerRunnable(runnable);
        this.description = description;
    }
    public void start() {
        if(isStarted.compareAndSet(false, true)) {
            long delay = delayMilliseconds != null ? delayMilliseconds : 0;
            ExecutorsManager.getInstance().getScheduledExecutorService().schedule(runnable, delay, TimeUnit.MILLISECONDS);
        }
    }

    public void stop() {
        if(!isStopped.compareAndSet(false, true)) {
            callFinishListener();
        }
    }

    private synchronized void callFinishListener() {
        if(this.finishListener != null) {
            AccurateTickFinishListener listener = this.finishListener;
            this.finishListener = null;
            listener.finished();
        }
    }

    public void setFinishListener(AccurateTickFinishListener finishListener) {
        this.finishListener = finishListener;
    }
}
