package io.tapdata.pdk.core.utils.timer;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;
import io.tapdata.pdk.core.executor.ExecutorsManager;

import java.util.Date;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class MaxFrequencyLimiter {
    public final static String TAG = MaxFrequencyLimiter.class.getSimpleName();
    public interface ActionListener {
        void action();
    }
    private long touch = 0;
    private long maxPeriodMilliseconds = 500;
    private ActionListener actionListener;
    private ScheduledFuture scheduledFuture;
    private boolean reschedule = false;
    private boolean hit = false;

    private Consumer<Throwable> errorConsumer;
    public MaxFrequencyLimiter errorConsumer(Consumer<Throwable> errorConsumer) {
        this.errorConsumer = errorConsumer;
        return this;
    }

    public static void main(String... args) {
        MaxFrequencyLimiter maxFrequencyLimiter = new MaxFrequencyLimiter(500, () -> {
            System.out.println("action at " + new Date().toGMTString());
        });

        for(int i = 0; i < 10000; i++ ) {
            maxFrequencyLimiter.touch();
            System.out.println("touch at " + new Date().toGMTString());
//            try {
//                Thread.sleep(10L);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
    }

    public void stop() {
        actionListener = null;
        if(scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
    }


    public MaxFrequencyLimiter(ActionListener actionListener) {
        this.actionListener = actionListener;
    }

    public MaxFrequencyLimiter(long maxPeriodMilliseconds, ActionListener actionListener) {
        this.maxPeriodMilliseconds = maxPeriodMilliseconds;
        this.actionListener = actionListener;
    }

    public void touch() {
        long currentTime = System.currentTimeMillis();
        long time = currentTime - touch;

        if(scheduledFuture == null) {
            synchronized (this) {
                if(scheduledFuture == null) {
                    if(time > maxPeriodMilliseconds) {
                        scheduledFuture = ExecutorsManager.getInstance().getScheduledExecutorService().schedule(this::handleScheduledAction, 0, TimeUnit.MILLISECONDS);
                    } else {
                        scheduledFuture = ExecutorsManager.getInstance().getScheduledExecutorService().schedule(this::handleScheduledAction, maxPeriodMilliseconds - time, TimeUnit.MILLISECONDS);
                    }
                }
            }
        } else if(reschedule) {
            hit = true;
        }
    }

    private void handleScheduledAction() {
        touch = System.currentTimeMillis();
        try {
            reschedule = true;
            action();
        } finally {
            synchronized (this) {
                if(hit) {//在action执行期间来的动作， 需要reschedule
                    hit = false;
                    scheduledFuture = ExecutorsManager.getInstance().getScheduledExecutorService().schedule(this::handleScheduledAction, maxPeriodMilliseconds, TimeUnit.MILLISECONDS);
                } else {
                    scheduledFuture = null;
                }
                reschedule = false;
            }
        }
    }

    private void action() {
        if(actionListener != null) {
            try {
                actionListener.action();
            } catch(Throwable throwable) {
                throwable.printStackTrace();
                TapUtils tapUtils = InstanceFactory.instance(TapUtils.class);
                TapLogger.error(TAG, "Action failed, " + (tapUtils != null ? tapUtils.getStackTrace(throwable) : throwable.getMessage()));
                if(errorConsumer != null)
                    errorConsumer.accept(throwable);
            }
        }
    }
}
