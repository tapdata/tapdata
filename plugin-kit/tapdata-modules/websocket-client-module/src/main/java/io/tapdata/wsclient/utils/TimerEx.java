package io.tapdata.wsclient.utils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class TimerEx {
    private static ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(3);

    public static ScheduledFuture<?> scheduleInSeconds(Runnable runnable, long delay) {
        return scheduledExecutorService.schedule(runnable, delay, TimeUnit.SECONDS);
    }

    public static ScheduledFuture<?> scheduleInSeconds(Runnable runnable, long delay, long period) {
        return scheduledExecutorService.scheduleAtFixedRate(runnable, delay, period, TimeUnit.SECONDS);
    }
}
